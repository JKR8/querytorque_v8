package com.qtcalcite;

import com.qtcalcite.calcite.DuckDBStatistics;
import com.qtcalcite.calcite.VolcanoOptimizer;
import com.qtcalcite.duckdb.DuckDBAdapter;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Volcano optimizer with TPC-DS Query 4.
 * Q4 is a 6-way self-join query where join ordering significantly impacts performance.
 *
 * Run with: ./gradlew test --tests "com.qtcalcite.TPCDSQ4Test" -Dtpcds.path=D:/TPC-DS
 */
public class TPCDSQ4Test {

    // Check environment variable first, then system property, then default
    private static final String TPCDS_PATH = System.getenv("TPCDS_PATH") != null
            ? System.getenv("TPCDS_PATH")
            : System.getProperty("tpcds.path", "/mnt/d/TPC-DS/tpcds_sf100.duckdb");
    private static DuckDBAdapter adapter;
    private static VolcanoOptimizer optimizer;
    private static boolean tpcdsAvailable = false;

    // TPC-DS Query 4 pattern (simplified for testing)
    // This query performs self-join on customer data across multiple years
    // Note: "year" is reserved in Calcite, so we use "dyear" alias
    private static final String TPCDS_Q4_SIMPLIFIED = """
        SELECT c_customer_id, c_first_name, c_last_name,
               SUM(CASE WHEN d_year = 2001 THEN ss_net_paid ELSE 0 END) as sales_2001,
               SUM(CASE WHEN d_year = 2002 THEN ss_net_paid ELSE 0 END) as sales_2002
        FROM customer
        JOIN store_sales ON c_customer_sk = ss_customer_sk
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        WHERE d_year IN (2001, 2002)
        GROUP BY c_customer_id, c_first_name, c_last_name
        HAVING SUM(CASE WHEN d_year = 2002 THEN ss_net_paid ELSE 0 END) >
               SUM(CASE WHEN d_year = 2001 THEN ss_net_paid ELSE 0 END)
        ORDER BY c_customer_id
        LIMIT 100
    """;

    // Simpler multi-join query for basic testing
    private static final String MULTI_JOIN_QUERY = """
        SELECT c.c_customer_id, c.c_first_name, c.c_last_name,
               SUM(ss1.ss_net_paid) as year1_total,
               SUM(ss2.ss_net_paid) as year2_total
        FROM customer c
        JOIN store_sales ss1 ON c.c_customer_sk = ss1.ss_customer_sk
        JOIN store_sales ss2 ON c.c_customer_sk = ss2.ss_customer_sk
        JOIN date_dim d1 ON ss1.ss_sold_date_sk = d1.d_date_sk
        JOIN date_dim d2 ON ss2.ss_sold_date_sk = d2.d_date_sk
        WHERE d1.d_year = 2001
          AND d2.d_year = 2002
        GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name
        LIMIT 100
    """;

    @BeforeAll
    static void setup() {
        try {
            adapter = new DuckDBAdapter(TPCDS_PATH);
            optimizer = new VolcanoOptimizer(adapter);
            tpcdsAvailable = true;
            System.out.println("TPC-DS database loaded from: " + TPCDS_PATH);
        } catch (Exception e) {
            System.err.println("TPC-DS database not available at " + TPCDS_PATH + ": " + e.getMessage());
            System.err.println("Skipping TPC-DS tests. Set -Dtpcds.path=<path> to enable.");
            tpcdsAvailable = false;
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (adapter != null) {
            adapter.close();
        }
    }

    @Test
    void testStatisticsAvailable() {
        Assumptions.assumeTrue(tpcdsAvailable, "TPC-DS database not available");

        DuckDBStatistics stats = optimizer.getStatistics();

        // Check key TPC-DS tables
        System.out.println("\n=== TPC-DS Table Statistics ===");
        for (String table : Arrays.asList("store_sales", "customer", "date_dim", "catalog_sales", "web_sales")) {
            long rowCount = stats.getRowCount(table);
            System.out.printf("%s: %,d rows%n", table, rowCount);
            if (stats.getTableNames().contains(table)) {
                assertTrue(rowCount > 0, table + " should have rows");
            }
        }
    }

    @Test
    void testMultiJoinOptimization() {
        Assumptions.assumeTrue(tpcdsAvailable, "TPC-DS database not available");

        System.out.println("\n=== Multi-Join Query Optimization ===");
        System.out.println("Query:\n" + MULTI_JOIN_QUERY);

        // Test with join optimization rules
        List<String> joinRules = Arrays.asList(
            "JOIN_COMMUTE",
            "JOIN_ASSOCIATE",
            "FILTER_INTO_JOIN",
            "FILTER_PROJECT_TRANSPOSE",
            "PROJECT_MERGE"
        );

        VolcanoOptimizer.OptimizationResult result = optimizer.optimize(MULTI_JOIN_QUERY, joinRules);

        System.out.println("\n=== Optimization Result ===");
        System.out.println(result.formatSummary());

        if (result.hasError()) {
            System.err.println("Error: " + result.getError());
        }

        assertFalse(result.hasError(), "Should optimize without error: " + result.getError());
        assertNotNull(result.getOptimizedCost(), "Should have cost estimate");

        // Verify cost is finite (metadata is working)
        assertFalse(result.getOptimizedCost().isInfinite(),
            "Cost should be finite - DuckDB statistics should be injected");

        System.out.println("Optimized Cost: " + result.getOptimizedCost());
    }

    @Test
    void testQ4Simplified() {
        Assumptions.assumeTrue(tpcdsAvailable, "TPC-DS database not available");

        System.out.println("\n=== TPC-DS Q4 (Simplified) Optimization ===");

        // Test with comprehensive rule set for complex query
        List<String> rules = Arrays.asList(
            "JOIN_COMMUTE",
            "JOIN_ASSOCIATE",
            "FILTER_INTO_JOIN",
            "FILTER_PROJECT_TRANSPOSE",
            "PROJECT_MERGE",
            "AGGREGATE_PROJECT_MERGE",
            "SORT_PROJECT_TRANSPOSE"
        );

        VolcanoOptimizer.OptimizationResult result = optimizer.optimize(TPCDS_Q4_SIMPLIFIED, rules);

        System.out.println("\n=== Q4 Optimization Result ===");
        System.out.println(result.formatSummary());

        if (result.hasError()) {
            System.err.println("Error: " + result.getError());
            // Q4 with CTEs may not be fully supported - check if it's a parsing issue
            if (result.getError().contains("Parse error")) {
                System.out.println("Note: CTE syntax may not be fully supported");
            }
        }
    }

    @Test
    void testJoinOrderingImpact() {
        Assumptions.assumeTrue(tpcdsAvailable, "TPC-DS database not available");

        System.out.println("\n=== Join Ordering Impact Test ===");

        // Simple 3-way join to test ordering
        String query = """
            SELECT c.c_customer_id, s.s_store_name, SUM(ss.ss_net_paid)
            FROM store_sales ss
            JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
            JOIN store s ON ss.ss_store_sk = s.s_store_sk
            WHERE s.s_city = 'Midway'
            GROUP BY c.c_customer_id, s.s_store_name
            LIMIT 100
        """;

        // Without join reordering
        List<String> basicRules = Arrays.asList("FILTER_PROJECT_TRANSPOSE", "PROJECT_MERGE");
        VolcanoOptimizer.OptimizationResult basicResult = optimizer.optimize(query, basicRules);

        // With join reordering
        List<String> joinRules = Arrays.asList(
            "JOIN_COMMUTE",
            "JOIN_ASSOCIATE",
            "FILTER_INTO_JOIN",
            "FILTER_PROJECT_TRANSPOSE",
            "PROJECT_MERGE"
        );
        VolcanoOptimizer.OptimizationResult joinResult = optimizer.optimize(query, joinRules);

        System.out.println("Without join reordering:");
        System.out.println("  Cost: " + (basicResult.hasError() ? basicResult.getError() : basicResult.getOptimizedCost()));

        System.out.println("\nWith join reordering:");
        System.out.println("  Cost: " + (joinResult.hasError() ? joinResult.getError() : joinResult.getOptimizedCost()));

        if (!basicResult.hasError() && !joinResult.hasError()) {
            System.out.println("\nBasic SQL:\n" + basicResult.getOptimizedSql());
            System.out.println("\nJoin-optimized SQL:\n" + joinResult.getOptimizedSql());
        }
    }
}
