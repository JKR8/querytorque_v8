package com.qtcalcite.calcite;

import com.qtcalcite.duckdb.DuckDBAdapter;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;

import java.sql.SQLException;
import java.util.*;

/**
 * Cost-based optimizer using Calcite's Volcano planner with DuckDB statistics.
 */
public class VolcanoOptimizer {

    private final DuckDBAdapter duckDBAdapter;
    private final RuleRegistry ruleRegistry;
    private final DuckDBStatistics statistics;
    private final SchemaPlus rootSchema;
    private final RelDataTypeFactory typeFactory;

    public VolcanoOptimizer(DuckDBAdapter duckDBAdapter) throws SQLException {
        this.duckDBAdapter = duckDBAdapter;
        this.ruleRegistry = new RuleRegistry();
        this.statistics = new DuckDBStatistics(duckDBAdapter);
        this.typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);

        // Build schema from DuckDB
        this.rootSchema = Frameworks.createRootSchema(true);
        DuckDBSchemaFactory schemaFactory = new DuckDBSchemaFactory(duckDBAdapter);
        try {
            schemaFactory.addToSchema(rootSchema, "DUCKDB");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load DuckDB schema", e);
        }
    }

    public OptimizationResult optimize(String sql, List<String> ruleNames) {
        try {
            // Parse SQL
            SqlParser.Config parserConfig = SqlParser.config()
                    .withCaseSensitive(false);
            SqlParser parser = SqlParser.create(sql, parserConfig);
            SqlNode sqlNode = parser.parseQuery();

            // Setup catalog reader
            Properties props = new Properties();
            props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
            CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);

            CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                    CalciteSchema.from(rootSchema),
                    Collections.singletonList("DUCKDB"),
                    typeFactory,
                    config
            );

            // Validate
            SqlValidator validator = SqlValidatorUtil.newValidator(
                    SqlStdOperatorTable.instance(),
                    catalogReader,
                    typeFactory,
                    SqlValidator.Config.DEFAULT
            );
            SqlNode validatedSql = validator.validate(sqlNode);

            // Create Volcano planner with cost model
            VolcanoPlanner planner = new VolcanoPlanner(
                    RelOptCostImpl.FACTORY,
                    Contexts.of(config)
            );

            // Register trait definitions
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

            // Add BINDABLE conversion rules (Logical -> Physical)
            // Tables implement ScannableTable, enabling these conversion rules
            planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
            planner.addRule(Bindables.BINDABLE_FILTER_RULE);
            planner.addRule(Bindables.BINDABLE_PROJECT_RULE);
            planner.addRule(Bindables.BINDABLE_JOIN_RULE);
            planner.addRule(Bindables.BINDABLE_AGGREGATE_RULE);
            planner.addRule(Bindables.BINDABLE_SORT_RULE);
            planner.addRule(Bindables.BINDABLE_VALUES_RULE);
            planner.addRule(Bindables.BINDABLE_WINDOW_RULE);

            // Add selected optimization rules
            for (String ruleName : ruleNames) {
                RelOptRule rule = ruleRegistry.getRule(ruleName);
                if (rule != null) {
                    planner.addRule(rule);
                }
            }

            // Add base transformation rules
            planner.addRule(CoreRules.PROJECT_MERGE);
            planner.addRule(CoreRules.FILTER_MERGE);
            planner.addRule(CoreRules.FILTER_PROJECT_TRANSPOSE);
            planner.addRule(CoreRules.PROJECT_REMOVE);

            // Convert SQL to RelNode
            RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

            // Set custom metadata provider with DuckDB statistics
            DuckDBRelMetadataProvider metadataProvider = new DuckDBRelMetadataProvider(statistics);
            cluster.setMetadataProvider(metadataProvider.createProvider());

            SqlToRelConverter converter = new SqlToRelConverter(
                    null,
                    validator,
                    catalogReader,
                    cluster,
                    StandardConvertletTable.INSTANCE,
                    SqlToRelConverter.config()
            );

            RelRoot relRoot = converter.convertQuery(validatedSql, false, true);
            RelNode logicalPlan = relRoot.rel;

            // Get original cost before optimization
            RelMetadataQuery mq = cluster.getMetadataQuery();
            RelOptCost originalCost = planner.getCost(logicalPlan, mq);

            // Set root with BINDABLE convention target
            planner.setRoot(planner.changeTraits(logicalPlan,
                    logicalPlan.getTraitSet().replace(BindableConvention.INSTANCE)));

            // Find best plan using cost-based optimization
            RelNode optimized = planner.findBestExp();

            // Get optimized cost
            RelOptCost optimizedCost = planner.getCost(optimized, mq);

            // Convert back to SQL
            String optimizedSql = relToSql(optimized);

            return new OptimizationResult(
                    sql,
                    optimizedSql,
                    ruleNames,
                    !sql.equals(optimizedSql),
                    optimizedCost,
                    originalCost
            );

        } catch (SqlParseException e) {
            return new OptimizationResult(sql, sql, ruleNames, false, null, null,
                    "Parse error: " + e.getMessage());
        } catch (Exception e) {
            return new OptimizationResult(sql, sql, ruleNames, false, null, null,
                    "Optimization error: " + e.getMessage());
        }
    }

    private String relToSql(RelNode relNode) {
        try {
            org.apache.calcite.rel.rel2sql.RelToSqlConverter converter =
                    new org.apache.calcite.rel.rel2sql.RelToSqlConverter(
                            org.apache.calcite.sql.dialect.AnsiSqlDialect.DEFAULT);
            org.apache.calcite.sql.SqlNode sqlNode = converter.visitRoot(relNode).asStatement();
            return sqlNode.toSqlString(org.apache.calcite.sql.dialect.AnsiSqlDialect.DEFAULT)
                    .getSql()
                    .replaceAll("\"?DUCKDB\"?\\.", "");
        } catch (Exception e) {
            return "-- Error converting to SQL: " + e.getMessage();
        }
    }

    public DuckDBStatistics getStatistics() {
        return statistics;
    }

    public static class OptimizationResult {
        private final String originalSql;
        private final String optimizedSql;
        private final List<String> rulesApplied;
        private final boolean queryChanged;
        private final RelOptCost optimizedCost;
        private final RelOptCost originalCost;
        private final String error;

        public OptimizationResult(String originalSql, String optimizedSql,
                                  List<String> rulesApplied, boolean queryChanged,
                                  RelOptCost optimizedCost, RelOptCost originalCost) {
            this(originalSql, optimizedSql, rulesApplied, queryChanged,
                    optimizedCost, originalCost, null);
        }

        public OptimizationResult(String originalSql, String optimizedSql,
                                  List<String> rulesApplied, boolean queryChanged,
                                  RelOptCost optimizedCost, RelOptCost originalCost,
                                  String error) {
            this.originalSql = originalSql;
            this.optimizedSql = optimizedSql;
            this.rulesApplied = rulesApplied;
            this.queryChanged = queryChanged;
            this.optimizedCost = optimizedCost;
            this.originalCost = originalCost;
            this.error = error;
        }

        public String getOriginalSql() { return originalSql; }
        public String getOptimizedSql() { return optimizedSql; }
        public List<String> getRulesApplied() { return rulesApplied; }
        public boolean isQueryChanged() { return queryChanged; }
        public RelOptCost getOptimizedCost() { return optimizedCost; }
        public RelOptCost getOriginalCost() { return originalCost; }
        public String getError() { return error; }
        public boolean hasError() { return error != null; }

        public String formatSummary() {
            StringBuilder sb = new StringBuilder();
            sb.append("Applied rules: ").append(String.join(", ", rulesApplied)).append("\n");
            sb.append("Query changed: ").append(queryChanged ? "Yes" : "No").append("\n");

            if (originalCost != null && optimizedCost != null) {
                sb.append("Original cost: ").append(originalCost).append("\n");
                sb.append("Optimized cost: ").append(optimizedCost).append("\n");
            }

            if (queryChanged) {
                sb.append("\nOriginal SQL:\n  ").append(originalSql.trim()).append("\n");
                sb.append("\nOptimized SQL:\n  ").append(optimizedSql.trim()).append("\n");
            }

            if (error != null) {
                sb.append("\nError: ").append(error);
            }

            return sb.toString();
        }
    }
}
