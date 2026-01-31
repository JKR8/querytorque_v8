package com.qtcalcite;

import com.qtcalcite.calcite.DuckDBStatistics;
import com.qtcalcite.calcite.VolcanoOptimizer;
import com.qtcalcite.duckdb.DuckDBAdapter;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for VolcanoOptimizer with metadata integration.
 */
public class VolcanoOptimizerTest {

    private static DuckDBAdapter adapter;
    private static VolcanoOptimizer optimizer;

    @BeforeAll
    static void setup() throws Exception {
        // Create in-memory DuckDB with test data
        adapter = new DuckDBAdapter(":memory:");
        Connection conn = adapter.getConnection();

        try (Statement stmt = conn.createStatement()) {
            // Create test tables similar to TPC-DS structure
            stmt.execute("""
                CREATE TABLE store_sales (
                    ss_sold_date_sk INTEGER,
                    ss_item_sk INTEGER,
                    ss_customer_sk INTEGER,
                    ss_store_sk INTEGER,
                    ss_quantity INTEGER,
                    ss_sales_price DECIMAL(7,2),
                    ss_net_profit DECIMAL(7,2)
                )
            """);

            stmt.execute("""
                CREATE TABLE customer (
                    c_customer_sk INTEGER PRIMARY KEY,
                    c_customer_id VARCHAR(16),
                    c_first_name VARCHAR(20),
                    c_last_name VARCHAR(30),
                    c_birth_year INTEGER
                )
            """);

            stmt.execute("""
                CREATE TABLE date_dim (
                    d_date_sk INTEGER PRIMARY KEY,
                    d_year INTEGER,
                    d_moy INTEGER,
                    d_dom INTEGER
                )
            """);

            stmt.execute("""
                CREATE TABLE store (
                    s_store_sk INTEGER PRIMARY KEY,
                    s_store_name VARCHAR(50),
                    s_city VARCHAR(60)
                )
            """);

            // Insert test data for statistics
            stmt.execute("""
                INSERT INTO store_sales
                SELECT
                    (i % 1000) + 1 as ss_sold_date_sk,
                    (i % 5000) + 1 as ss_item_sk,
                    (i % 1000) + 1 as ss_customer_sk,
                    (i % 100) + 1 as ss_store_sk,
                    (i % 10) + 1 as ss_quantity,
                    (random() * 100)::DECIMAL(7,2) as ss_sales_price,
                    (random() * 50)::DECIMAL(7,2) as ss_net_profit
                FROM generate_series(1, 10000) as t(i)
            """);

            stmt.execute("""
                INSERT INTO customer
                SELECT
                    i as c_customer_sk,
                    'CUST' || i as c_customer_id,
                    'First' || (i % 100) as c_first_name,
                    'Last' || (i % 100) as c_last_name,
                    1950 + (i % 60) as c_birth_year
                FROM generate_series(1, 1000) as t(i)
            """);

            stmt.execute("""
                INSERT INTO date_dim
                SELECT
                    i as d_date_sk,
                    2000 + (i / 365) as d_year,
                    ((i % 365) / 30) + 1 as d_moy,
                    (i % 30) + 1 as d_dom
                FROM generate_series(1, 1000) as t(i)
            """);

            stmt.execute("""
                INSERT INTO store
                SELECT
                    i as s_store_sk,
                    'Store ' || i as s_store_name,
                    'City ' || (i % 20) as s_city
                FROM generate_series(1, 100) as t(i)
            """);
        }

        optimizer = new VolcanoOptimizer(adapter);
    }

    @AfterAll
    static void teardown() throws Exception {
        if (adapter != null) {
            adapter.close();
        }
    }

    @Test
    void testStatisticsLoaded() {
        DuckDBStatistics stats = optimizer.getStatistics();

        // Verify tables are loaded
        assertTrue(stats.getTableNames().contains("store_sales"));
        assertTrue(stats.getTableNames().contains("customer"));
        assertTrue(stats.getTableNames().contains("date_dim"));

        // Verify row counts are loaded (approximately)
        long storeSalesRows = stats.getRowCount("store_sales");
        assertTrue(storeSalesRows > 0, "store_sales should have rows: " + storeSalesRows);

        long customerRows = stats.getRowCount("customer");
        assertTrue(customerRows > 0, "customer should have rows: " + customerRows);
    }

    @Test
    void testSimpleQueryOptimization() {
        String sql = "SELECT * FROM customer WHERE c_customer_sk = 1";

        VolcanoOptimizer.OptimizationResult result = optimizer.optimize(sql, Collections.emptyList());

        // Print result first for debugging
        System.out.println("Simple query result:");
        System.out.println(result.formatSummary());
        if (result.hasError()) {
            System.err.println("ERROR: " + result.getError());
        }

        assertFalse(result.hasError(), "Should not have error: " + result.getError());
        assertNotNull(result.getOptimizedSql(), "Should have optimized SQL");
        assertNotNull(result.getOptimizedCost(), "Should have optimized cost");
    }

    @Test
    void testJoinOptimization() {
        String sql = """
            SELECT c.c_first_name, c.c_last_name, SUM(ss.ss_sales_price) as total_sales
            FROM store_sales ss
            JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
            WHERE ss.ss_quantity > 5
            GROUP BY c.c_first_name, c.c_last_name
        """;

        List<String> rules = Arrays.asList(
            "FILTER_PROJECT_TRANSPOSE",
            "PROJECT_MERGE",
            "AGGREGATE_PROJECT_MERGE"
        );

        VolcanoOptimizer.OptimizationResult result = optimizer.optimize(sql, rules);

        assertFalse(result.hasError(), "Should not have error: " + result.getError());
        assertNotNull(result.getOptimizedCost(), "Should have cost estimate");

        System.out.println("Join query result:");
        System.out.println(result.formatSummary());
    }

    @Test
    void testMultiJoinOptimization() {
        // Similar to TPC-DS Q4 pattern - multiple self-joins
        String sql = """
            SELECT c.c_customer_id, c.c_first_name, c.c_last_name
            FROM customer c
            JOIN store_sales ss1 ON c.c_customer_sk = ss1.ss_customer_sk
            JOIN store_sales ss2 ON c.c_customer_sk = ss2.ss_customer_sk
            JOIN date_dim d1 ON ss1.ss_sold_date_sk = d1.d_date_sk
            JOIN date_dim d2 ON ss2.ss_sold_date_sk = d2.d_date_sk
            WHERE d1.d_year = 2001
              AND d2.d_year = 2002
        """;

        List<String> rules = Arrays.asList(
            "JOIN_COMMUTE",
            "JOIN_ASSOCIATE",
            "FILTER_INTO_JOIN",
            "FILTER_PROJECT_TRANSPOSE"
        );

        VolcanoOptimizer.OptimizationResult result = optimizer.optimize(sql, rules);

        assertFalse(result.hasError(), "Should not have error: " + result.getError());

        System.out.println("Multi-join query result:");
        System.out.println(result.formatSummary());

        // Verify cost is computed (not infinite)
        if (result.getOptimizedCost() != null) {
            assertFalse(result.getOptimizedCost().isInfinite(),
                "Cost should not be infinite - metadata should be working");
        }
    }

    @Test
    void testCostComparison() {
        String sql = """
            SELECT c.c_first_name, SUM(ss.ss_sales_price)
            FROM store_sales ss
            JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
            JOIN store s ON ss.ss_store_sk = s.s_store_sk
            WHERE s.s_city = 'City 1'
            GROUP BY c.c_first_name
        """;

        // Test with different rule sets to see cost impact
        List<String> basicRules = Collections.emptyList();
        List<String> optimizationRules = Arrays.asList(
            "FILTER_INTO_JOIN",
            "FILTER_PROJECT_TRANSPOSE",
            "PROJECT_MERGE"
        );

        VolcanoOptimizer.OptimizationResult basicResult = optimizer.optimize(sql, basicRules);
        VolcanoOptimizer.OptimizationResult optimizedResult = optimizer.optimize(sql, optimizationRules);

        System.out.println("Cost comparison:");
        System.out.println("Basic: " + (basicResult.hasError() ? basicResult.getError() : basicResult.getOptimizedCost()));
        System.out.println("Optimized: " + (optimizedResult.hasError() ? optimizedResult.getError() : optimizedResult.getOptimizedCost()));

        assertFalse(basicResult.hasError(), "Basic should not error: " + basicResult.getError());
        assertFalse(optimizedResult.hasError(), "Optimized should not error: " + optimizedResult.getError());
    }
}
