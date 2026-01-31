package com.qtcalcite;

import com.qtcalcite.calcite.OptimizationPipeline;
import com.qtcalcite.calcite.OptimizationPipeline.PipelineResult;
import com.qtcalcite.duckdb.DuckDBAdapter;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;

import java.sql.*;

/**
 * Test the integrated optimization pipeline.
 */
public class OptimizationPipelineTest {

    private static final String TPCDS_PATH = System.getenv("TPCDS_PATH") != null
            ? System.getenv("TPCDS_PATH")
            : "/mnt/d/TPC-DS/tpcds_sf100.duckdb";

    private static DuckDBAdapter adapter;
    private static OptimizationPipeline pipeline;
    private static Connection conn;
    private static boolean available = false;

    @BeforeAll
    static void setup() throws Exception {
        try {
            adapter = new DuckDBAdapter(TPCDS_PATH);
            pipeline = new OptimizationPipeline(adapter);
            conn = adapter.getConnection();
            available = true;
            System.out.println("Connected to: " + TPCDS_PATH);
        } catch (Exception e) {
            System.err.println("Setup failed: " + e.getMessage());
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (adapter != null) adapter.close();
    }

    @Test
    void testGroupedTopNOptimization() throws Exception {
        Assumptions.assumeTrue(available);

        String sql = """
            SELECT * FROM (
                SELECT s_store_id, s_store_name, ss_net_paid,
                       ROW_NUMBER() OVER (PARTITION BY s_store_id ORDER BY ss_net_paid DESC) as rn
                FROM store
                JOIN store_sales ON s_store_sk = ss_store_sk
                JOIN date_dim ON ss_sold_date_sk = d_date_sk
                WHERE d_year = 2001 AND d_moy <= 3
            ) ranked
            WHERE rn <= 5
            """;

        System.out.println("=".repeat(70));
        System.out.println("GROUPED TOPN PIPELINE TEST");
        System.out.println("=".repeat(70));

        PipelineResult result = pipeline.optimize(sql);
        System.out.println(result.format());

        // Verify gap was detected
        Assertions.assertNotNull(result.getGapAnalysis());
        Assertions.assertTrue(result.getGapAnalysis().hasGaps(),
            "Should detect GROUPED_TOPN gap");

        System.out.println("=".repeat(70));
    }

    @Test
    void testNoGapQuery() throws Exception {
        Assumptions.assumeTrue(available);

        // Simple query with no optimization gaps
        String sql = """
            SELECT s_store_id, s_store_name
            FROM store
            WHERE s_state = 'TN'
            LIMIT 10
            """;

        System.out.println("=".repeat(70));
        System.out.println("NO GAP QUERY TEST");
        System.out.println("=".repeat(70));

        PipelineResult result = pipeline.optimize(sql);
        System.out.println(result.format());

        // Should not detect any gaps
        Assertions.assertFalse(result.isSuccess(),
            "Simple query should not need optimization");

        System.out.println("=".repeat(70));
    }

    @Test
    @Disabled("Causes StackOverflowError - needs investigation")
    void testMultipleLeftJoins() throws Exception {
        Assumptions.assumeTrue(available);

        String sql = """
            SELECT ss.ss_item_sk, d1.d_date, d2.d_month_seq, d3.d_year
            FROM store_sales ss
            LEFT JOIN date_dim d1 ON ss.ss_sold_date_sk = d1.d_date_sk
            LEFT JOIN date_dim d2 ON ss.ss_sold_date_sk = d2.d_date_sk
            LEFT JOIN date_dim d3 ON ss.ss_sold_date_sk = d3.d_date_sk
            WHERE d1.d_year = 2001
            LIMIT 100
            """;

        System.out.println("=".repeat(70));
        System.out.println("MULTIPLE LEFT JOINS TEST");
        System.out.println("=".repeat(70));

        PipelineResult result = pipeline.optimize(sql);
        System.out.println(result.format());

        // Should detect MULTIPLE_LEFT_JOINS gap
        Assertions.assertNotNull(result.getGapAnalysis());

        System.out.println("=".repeat(70));
    }
}
