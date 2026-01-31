package com.qtcalcite.calcite;

import com.qtcalcite.duckdb.DuckDBAdapter;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Core Calcite-based query optimizer.
 * Handles SQL parsing, relational algebra conversion, rule application, and SQL regeneration.
 */
public class CalciteOptimizer {

    private final DuckDBAdapter duckDBAdapter;
    private final RuleRegistry ruleRegistry;
    private final SchemaPlus rootSchema;
    private final FrameworkConfig frameworkConfig;

    public CalciteOptimizer(DuckDBAdapter duckDBAdapter) throws SQLException {
        this.duckDBAdapter = duckDBAdapter;
        this.ruleRegistry = new RuleRegistry();

        // Create root schema
        this.rootSchema = Frameworks.createRootSchema(true);

        // Add DuckDB schema
        DuckDBSchemaFactory schemaFactory = new DuckDBSchemaFactory(duckDBAdapter);
        schemaFactory.addToSchema(rootSchema, "DUCKDB");

        // Build framework config
        this.frameworkConfig = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema("DUCKDB"))
                .parserConfig(SqlParser.config()
                        .withCaseSensitive(false))
                .build();
    }

    public RuleRegistry getRuleRegistry() {
        return ruleRegistry;
    }

    /**
     * Parse SQL to Calcite SqlNode.
     */
    public SqlNode parseSql(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, frameworkConfig.getParserConfig());
        return parser.parseQuery();
    }

    /**
     * Convert SQL to RelNode (relational algebra).
     */
    public RelNode sqlToRelNode(String sql) throws SqlParseException {
        SqlNode sqlNode = parseSql(sql);

        // Get type factory and create catalog reader
        RelDataTypeFactory typeFactory = frameworkConfig.getTypeSystem() != null
                ? new org.apache.calcite.jdbc.JavaTypeFactoryImpl(frameworkConfig.getTypeSystem())
                : new org.apache.calcite.jdbc.JavaTypeFactoryImpl();

        Properties props = new Properties();
        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema.getSubSchema("DUCKDB")),
                Collections.singletonList(""),
                typeFactory,
                config
        );

        // Create validator
        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                SqlValidator.Config.DEFAULT
        );

        SqlNode validatedNode = validator.validate(sqlNode);

        // Create RelOptCluster
        RelOptPlanner planner = new HepPlanner(HepProgram.builder().build());
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        // Convert to RelNode
        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config()
        );

        RelRoot relRoot = converter.convertQuery(validatedNode, false, true);
        return relRoot.rel;
    }

    /**
     * Apply optimization rules to a RelNode.
     */
    public RelNode applyRules(RelNode relNode, List<String> ruleNames) {
        if (ruleNames == null || ruleNames.isEmpty()) {
            return relNode;
        }

        // Build HEP program with selected rules
        HepProgramBuilder programBuilder = HepProgram.builder();

        for (String ruleName : ruleNames) {
            RelOptRule rule = ruleRegistry.getRule(ruleName);
            if (rule != null) {
                programBuilder.addRuleInstance(rule);
            }
        }

        HepProgram program = programBuilder.build();
        HepPlanner planner = new HepPlanner(program);

        planner.setRoot(relNode);
        return planner.findBestExp();
    }

    // DuckDB-compatible SQL dialect
    private static final SqlDialect DUCKDB_DIALECT = new SqlDialect(
            SqlDialect.EMPTY_CONTEXT
                    .withDatabaseProduct(DatabaseProduct.UNKNOWN)
                    .withIdentifierQuoteString("\"")
                    .withUnquotedCasing(org.apache.calcite.avatica.util.Casing.TO_LOWER)
                    .withQuotedCasing(org.apache.calcite.avatica.util.Casing.UNCHANGED)
    );

    /**
     * Convert RelNode back to SQL string.
     */
    public String relNodeToSql(RelNode relNode) {
        RelToSqlConverter converter = new RelToSqlConverter(DUCKDB_DIALECT);
        SqlNode sqlNode = converter.visitRoot(relNode).asStatement();
        String sql = sqlNode.toSqlString(DUCKDB_DIALECT).getSql();
        // Remove schema prefix (DUCKDB.) that Calcite adds
        sql = sql.replaceAll("\"?DUCKDB\"?\\.", "");
        // Remove double quotes around identifiers for cleaner output
        sql = sql.replace("\"", "");
        return sql;
    }

    /**
     * Full optimization pipeline: SQL -> RelNode -> apply rules -> SQL
     */
    public OptimizationResult optimize(String sql, List<String> ruleNames) throws SqlParseException {
        // Parse and convert to RelNode
        RelNode originalRelNode = sqlToRelNode(sql);
        String originalPlan = originalRelNode.explain();

        // Apply rules
        RelNode optimizedRelNode = applyRules(originalRelNode, ruleNames);
        String optimizedPlan = optimizedRelNode.explain();

        // Convert back to SQL
        String optimizedSql = relNodeToSql(optimizedRelNode);

        // Check if optimization made changes
        boolean changed = !originalPlan.equals(optimizedPlan);

        return new OptimizationResult(
                sql,
                optimizedSql,
                originalPlan,
                optimizedPlan,
                ruleNames,
                changed
        );
    }

    /**
     * Result of query optimization.
     */
    public static class OptimizationResult {
        private final String originalSql;
        private final String optimizedSql;
        private final String originalPlan;
        private final String optimizedPlan;
        private final List<String> appliedRules;
        private final boolean changed;

        public OptimizationResult(String originalSql, String optimizedSql,
                                  String originalPlan, String optimizedPlan,
                                  List<String> appliedRules, boolean changed) {
            this.originalSql = originalSql;
            this.optimizedSql = optimizedSql;
            this.originalPlan = originalPlan;
            this.optimizedPlan = optimizedPlan;
            this.appliedRules = appliedRules;
            this.changed = changed;
        }

        public String getOriginalSql() { return originalSql; }
        public String getOptimizedSql() { return optimizedSql; }
        public String getOriginalPlan() { return originalPlan; }
        public String getOptimizedPlan() { return optimizedPlan; }
        public List<String> getAppliedRules() { return appliedRules; }
        public boolean isChanged() { return changed; }

        public String formatSummary() {
            StringBuilder sb = new StringBuilder();
            sb.append("Applied rules: ").append(String.join(", ", appliedRules)).append("\n");
            sb.append("Query changed: ").append(changed ? "Yes" : "No").append("\n");
            if (changed) {
                sb.append("\nOriginal SQL:\n  ").append(originalSql).append("\n");
                sb.append("\nOptimized SQL:\n  ").append(optimizedSql).append("\n");
            }
            return sb.toString();
        }
    }
}
