package com.qtcalcite;

import com.qtcalcite.rules.FkJoinEliminationRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for FK join elimination rule.
 */
public class FkJoinEliminationRuleTest {

    private static FrameworkConfig frameworkConfig;
    private static SchemaPlus rootSchema;

    @BeforeAll
    static void setup() {
        rootSchema = Frameworks.createRootSchema(true);

        rootSchema.add("STORE_SALES", new SimpleTable(
            new String[]{"SS_CUSTOMER_SK", "SS_NET_PAID"},
            new SqlTypeName[]{SqlTypeName.INTEGER, SqlTypeName.DECIMAL}
        ));

        rootSchema.add("CUSTOMER", new SimpleTable(
            new String[]{"C_CUSTOMER_SK", "C_LAST_NAME"},
            new SqlTypeName[]{SqlTypeName.INTEGER, SqlTypeName.VARCHAR}
        ));

        frameworkConfig = Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .parserConfig(SqlParser.config().withCaseSensitive(false))
            .build();
    }

    @Test
    void testJoinEliminatedWhenRightColumnsUnused() throws Exception {
        String sql = """
            SELECT ss_customer_sk, SUM(ss_net_paid) AS sales
            FROM store_sales ss
            JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
            GROUP BY ss_customer_sk
            """;

        RelNode relNode = parseToRelNode(sql);

        HepProgram program = new HepProgramBuilder()
            .addRuleInstance(FkJoinEliminationRule.INSTANCE)
            .build();

        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(relNode);
        RelNode optimized = planner.findBestExp();

        String plan = optimized.explain();
        assertFalse(plan.contains("LogicalJoin"), "Join should be eliminated");
        assertTrue(plan.contains("LogicalFilter"), "Filter should be added to preserve semantics");
    }

    @Test
    void testJoinNotEliminatedWhenRightColumnsUsed() throws Exception {
        String sql = """
            SELECT ss_customer_sk, c_last_name, SUM(ss_net_paid) AS sales
            FROM store_sales ss
            JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
            GROUP BY ss_customer_sk, c_last_name
            """;

        RelNode relNode = parseToRelNode(sql);

        HepProgram program = new HepProgramBuilder()
            .addRuleInstance(FkJoinEliminationRule.INSTANCE)
            .build();

        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(relNode);
        RelNode optimized = planner.findBestExp();

        String plan = optimized.explain();
        assertTrue(plan.contains("LogicalJoin"), "Join should remain when right columns are used");
    }

    private RelNode parseToRelNode(String sql) throws Exception {
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        SqlNode parsed = planner.parse(sql);
        SqlNode validated = planner.validate(parsed);
        RelRoot relRoot = planner.rel(validated);
        planner.close();
        return relRoot.rel;
    }

    private static class SimpleTable extends AbstractTable {
        private final String[] columnNames;
        private final SqlTypeName[] columnTypes;

        SimpleTable(String[] columnNames, SqlTypeName[] columnTypes) {
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (int i = 0; i < columnNames.length; i++) {
                builder.add(columnNames[i], columnTypes[i]);
            }
            return builder.build();
        }
    }
}
