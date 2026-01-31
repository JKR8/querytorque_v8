package com.qtcalcite;

import com.qtcalcite.calcite.DuckDBStatistics;
import com.qtcalcite.detector.GapDetector;
import com.qtcalcite.detector.GapDetector.*;
import com.qtcalcite.duckdb.DuckDBAdapter;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for GapDetector - verifies detection of DuckDB optimizer gaps.
 */
public class GapDetectorTest {

    private static final String TPCDS_PATH = System.getenv("TPCDS_PATH") != null
            ? System.getenv("TPCDS_PATH")
            : System.getProperty("tpcds.path", "/mnt/d/TPC-DS/tpcds_sf100.duckdb");

    private static DuckDBAdapter adapter;
    private static DuckDBStatistics statistics;
    private static GapDetector detector;
    private static boolean available = false;

    @BeforeAll
    static void setup() {
        try {
            adapter = new DuckDBAdapter(TPCDS_PATH);
            statistics = new DuckDBStatistics(adapter);
            detector = new GapDetector(adapter, statistics);
            available = true;
            System.out.println("GapDetector test setup complete. Database: " + TPCDS_PATH);
        } catch (Exception e) {
            System.err.println("TPC-DS not available: " + e.getMessage());
            available = false;
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (adapter != null) adapter.close();
    }

    // ==================== Gap 1: Join Order ====================

    @Test
    void testDetectJoinOrderGap() {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        // TPC-DS Q37 pattern - multi-way join with large tables
        String sql = """
            SELECT i_item_id, i_item_desc, i_current_price
            FROM item, inventory, date_dim, catalog_sales
            WHERE i_current_price BETWEEN 22 AND 52
              AND inv_item_sk = i_item_sk
              AND d_date_sk = inv_date_sk
              AND d_date BETWEEN '2001-06-02' AND '2001-08-02'
              AND cs_item_sk = i_item_sk
            GROUP BY i_item_id, i_item_desc, i_current_price
            ORDER BY i_item_id
            LIMIT 100
            """;

        GapAnalysis analysis = detector.analyze(sql);
        System.out.println(analysis.format());

        assertTrue(analysis.hasGap(Gap.JOIN_ORDER),
            "Should detect JOIN_ORDER gap for multi-way join with large tables");

        // Should recommend Volcano optimizer
        assertEquals(OptimizerType.VOLCANO, analysis.getRecommendedOptimizer());

        // Should include join optimization rules
        assertTrue(analysis.getAllRecommendedRules().contains("JOIN_COMMUTE"));
        assertTrue(analysis.getAllRecommendedRules().contains("MULTI_JOIN_OPTIMIZE"));
    }

    // ==================== Gap 2: Semi-Join Inequality ====================

    @Test
    void testDetectSemiJoinInequalityGap() {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        // TPC-H Q21 pattern - EXISTS with inequality
        String sql = """
            SELECT s_name, COUNT(*) as numwait
            FROM supplier, lineitem l1, orders, nation
            WHERE s_suppkey = l1.l_suppkey
              AND o_orderkey = l1.l_orderkey
              AND EXISTS (
                  SELECT 1 FROM lineitem l2
                  WHERE l2.l_orderkey = l1.l_orderkey
                    AND l2.l_suppkey <> l1.l_suppkey
              )
            GROUP BY s_name
            LIMIT 100
            """;

        GapAnalysis analysis = detector.analyze(sql);
        System.out.println(analysis.format());

        assertTrue(analysis.hasGap(Gap.SEMI_JOIN_INEQUALITY),
            "Should detect SEMI_JOIN_INEQUALITY gap for EXISTS with <>");

        // Should recommend HEP optimizer for this pattern
        assertTrue(analysis.getAllRecommendedRules().contains("PROJECT_TO_SEMI_JOIN") ||
                   analysis.getAllRecommendedRules().contains("JOIN_TO_SEMI_JOIN"));
    }

    @Test
    void testDetectSemiJoinWithNotExists() {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        String sql = """
            SELECT c_customer_id
            FROM customer c
            WHERE NOT EXISTS (
                SELECT 1 FROM store_sales ss
                WHERE ss.ss_customer_sk = c.c_customer_sk
                  AND ss.ss_net_paid > 1000
            )
            LIMIT 100
            """;

        GapAnalysis analysis = detector.analyze(sql);
        System.out.println(analysis.format());

        // NOT EXISTS with > is also an inequality semi-join
        assertTrue(analysis.hasGap(Gap.SEMI_JOIN_INEQUALITY),
            "Should detect SEMI_JOIN_INEQUALITY gap for NOT EXISTS with inequality");
    }

    // ==================== Gap 3: Grouped TopN ====================

    @Test
    void testDetectGroupedTopNGap() {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        // ROW_NUMBER with PARTITION BY and filter
        String sql = """
            SELECT * FROM (
                SELECT c_customer_id, ss_net_paid,
                       ROW_NUMBER() OVER (PARTITION BY c_customer_id ORDER BY ss_net_paid DESC) as rn
                FROM customer
                JOIN store_sales ON c_customer_sk = ss_customer_sk
            ) ranked
            WHERE rn <= 3
            """;

        GapAnalysis analysis = detector.analyze(sql);
        System.out.println(analysis.format());

        assertTrue(analysis.hasGap(Gap.GROUPED_TOPN),
            "Should detect GROUPED_TOPN gap for ROW_NUMBER + PARTITION BY + filter");

        // Should note QUALIFY alternative
        DetectedGap gap = analysis.getGaps().stream()
            .filter(g -> g.getGap() == Gap.GROUPED_TOPN)
            .findFirst().orElse(null);

        assertNotNull(gap);
        assertNotNull(gap.getNote());
        assertTrue(gap.getNote().contains("QUALIFY") || gap.getNote().contains("LATERAL"));
    }

    // ==================== Gap 4: CTE + LIMIT ====================

    @Test
    void testDetectCTELimitGap() {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        // CTE with ORDER BY inside, LIMIT outside
        String sql = """
            WITH ranked_customers AS (
                SELECT c_customer_id, c_first_name,
                       SUM(ss_net_paid) as total_spend
                FROM customer
                JOIN store_sales ON c_customer_sk = ss_customer_sk
                GROUP BY c_customer_id, c_first_name
                ORDER BY total_spend DESC
            )
            SELECT * FROM ranked_customers LIMIT 100
            """;

        GapAnalysis analysis = detector.analyze(sql);
        System.out.println(analysis.format());

        assertTrue(analysis.hasGap(Gap.CTE_LIMIT),
            "Should detect CTE_LIMIT gap for ORDER BY in CTE + external LIMIT");

        // Should recommend HEP with sort/limit rules
        assertTrue(analysis.getAllRecommendedRules().contains("SORT_PROJECT_TRANSPOSE") ||
                   analysis.getAllRecommendedRules().contains("LIMIT_MERGE"));
    }

    // ==================== Gap 5: Set Operations ====================

    @Test
    void testDetectIntersectGap() {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        String sql = """
            SELECT c_customer_id FROM customer WHERE c_birth_year = 1980
            INTERSECT
            SELECT ss_customer_sk FROM store_sales WHERE ss_net_paid > 100
            """;

        GapAnalysis analysis = detector.analyze(sql);
        System.out.println(analysis.format());

        assertTrue(analysis.hasGap(Gap.SET_OP_NO_SHORTCIRCUIT),
            "Should detect SET_OP_NO_SHORTCIRCUIT gap for INTERSECT");
    }

    @Test
    void testDetectExceptGap() {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        String sql = """
            SELECT c_customer_id FROM customer
            EXCEPT
            SELECT ss_customer_sk FROM store_sales WHERE ss_quantity_on_hand = 0
            """;

        GapAnalysis analysis = detector.analyze(sql);
        System.out.println(analysis.format());

        assertTrue(analysis.hasGap(Gap.SET_OP_NO_SHORTCIRCUIT),
            "Should detect SET_OP_NO_SHORTCIRCUIT gap for EXCEPT");
    }

    // ==================== Gap 6: Multiple LEFT JOINs ====================

    @Test
    void testDetectMultipleLeftJoinsGap() {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        String sql = """
            SELECT ss.*, d1.d_date, d2.d_date, d3.d_date
            FROM store_sales ss
            LEFT JOIN date_dim d1 ON ss.ss_sold_date_sk = d1.d_date_sk
            LEFT JOIN date_dim d2 ON ss.ss_sold_date_sk = d2.d_date_sk
            LEFT JOIN date_dim d3 ON ss.ss_sold_date_sk = d3.d_date_sk
            WHERE d1.d_year = 2001
            LIMIT 100
            """;

        GapAnalysis analysis = detector.analyze(sql);
        System.out.println(analysis.format());

        assertTrue(analysis.hasGap(Gap.MULTIPLE_LEFT_JOINS),
            "Should detect MULTIPLE_LEFT_JOINS gap for 3+ LEFT JOINs");

        // Should recommend Volcano optimizer
        assertEquals(OptimizerType.VOLCANO, analysis.getRecommendedOptimizer());

        // Should note null-rejecting predicate opportunity
        DetectedGap gap = analysis.getGaps().stream()
            .filter(g -> g.getGap() == Gap.MULTIPLE_LEFT_JOINS)
            .findFirst().orElse(null);

        assertNotNull(gap);
        assertNotNull(gap.getNote());
        assertTrue(gap.getNote().toLowerCase().contains("null-rejecting"));
    }

    // ==================== No Gap Detection ====================

    @Test
    void testNoGapForSimpleQuery() {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        // Simple query that shouldn't trigger any gaps
        String sql = """
            SELECT c_customer_id, c_first_name
            FROM customer
            WHERE c_birth_year = 1980
            LIMIT 10
            """;

        GapAnalysis analysis = detector.analyze(sql);
        System.out.println(analysis.format());

        // Should not detect any major gaps (small table, simple query)
        // May still have warnings from EXPLAIN
        assertTrue(analysis.getGaps().isEmpty() ||
                   analysis.getGaps().stream().allMatch(g ->
                       g.getGap() != Gap.SEMI_JOIN_INEQUALITY &&
                       g.getGap() != Gap.GROUPED_TOPN &&
                       g.getGap() != Gap.CTE_LIMIT &&
                       g.getGap() != Gap.MULTIPLE_LEFT_JOINS),
            "Simple query should not trigger pattern-based gaps");
    }

    // ==================== Comprehensive Test ====================

    @Test
    void testAnalysisReport() {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        // Complex query that might trigger multiple gaps
        String sql = """
            SELECT c.c_customer_id, s.s_store_name, SUM(ss.ss_net_paid) as total_sales
            FROM store_sales ss
            JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
            JOIN store s ON ss.ss_store_sk = s.s_store_sk
            JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
            WHERE d.d_year = 2001
              AND c.c_birth_year BETWEEN 1960 AND 1980
            GROUP BY c.c_customer_id, s.s_store_name
            ORDER BY total_sales DESC
            LIMIT 100
            """;

        GapAnalysis analysis = detector.analyze(sql);
        String report = analysis.format();

        System.out.println(report);

        // Report should be well-formed
        assertTrue(report.contains("GAP ANALYSIS REPORT"));
        assertTrue(report.contains("RECOMMENDATION"));

        // Should have either gaps or "No known gaps"
        assertTrue(report.contains("DETECTED GAPS") || report.contains("No known"));
    }
}
