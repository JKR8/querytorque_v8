package com.qtcalcite;

import com.qtcalcite.calcite.DuckDBStatistics;
import com.qtcalcite.detector.GapDetector;
import com.qtcalcite.duckdb.DuckDBAdapter;
import org.junit.jupiter.api.*;

/**
 * Test GapDetector on real TPC-DS queries.
 */
public class TPCDSGapAnalysisTest {

    private static final String TPCDS_PATH = System.getenv("TPCDS_PATH") != null
            ? System.getenv("TPCDS_PATH")
            : "/mnt/d/TPC-DS/tpcds_sf100.duckdb";

    private static DuckDBAdapter adapter;
    private static DuckDBStatistics stats;
    private static GapDetector detector;
    private static boolean available = false;

    @BeforeAll
    static void setup() {
        try {
            adapter = new DuckDBAdapter(TPCDS_PATH);
            stats = new DuckDBStatistics(adapter);
            detector = new GapDetector(adapter, stats);
            available = true;
            System.out.println("Connected to: " + TPCDS_PATH);
        } catch (Exception e) {
            System.err.println("TPC-DS not available: " + e.getMessage());
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (adapter != null) adapter.close();
    }

    @Test
    void analyzeQ4_YearOverYearComparison() {
        Assumptions.assumeTrue(available);

        String q4 = """
            WITH year_total AS (
                SELECT c_customer_id customer_id,
                       c_first_name customer_first_name,
                       c_last_name customer_last_name,
                       d_year as year,
                       SUM(ss_net_paid) year_total
                FROM customer, store_sales, date_dim
                WHERE c_customer_sk = ss_customer_sk
                  AND ss_sold_date_sk = d_date_sk
                  AND d_year IN (2001, 2002)
                GROUP BY c_customer_id, c_first_name, c_last_name, d_year
            )
            SELECT t_s_secyear.customer_id
            FROM year_total t_s_firstyear, year_total t_s_secyear
            WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
              AND t_s_firstyear.year = 2001
              AND t_s_secyear.year = 2002
              AND t_s_secyear.year_total > t_s_firstyear.year_total
            ORDER BY t_s_secyear.customer_id
            LIMIT 100
            """;

        System.out.println("\n" + "=".repeat(70));
        System.out.println("TPC-DS Q4 (Year-over-Year Customer Comparison)");
        System.out.println("=".repeat(70));
        System.out.println(detector.analyze(q4).format());
    }

    @Test
    void analyzeQ21_InventoryAnalysis() {
        Assumptions.assumeTrue(available);

        String q21 = """
            SELECT w_warehouse_name, i_item_id,
                   SUM(CASE WHEN d_date < '2000-03-11' THEN inv_quantity_on_hand ELSE 0 END) as inv_before,
                   SUM(CASE WHEN d_date >= '2000-03-11' THEN inv_quantity_on_hand ELSE 0 END) as inv_after
            FROM inventory, warehouse, item, date_dim
            WHERE i_current_price BETWEEN 0.99 AND 1.49
              AND i_item_sk = inv_item_sk
              AND inv_warehouse_sk = w_warehouse_sk
              AND inv_date_sk = d_date_sk
              AND d_date BETWEEN '2000-02-10' AND '2000-04-10'
            GROUP BY w_warehouse_name, i_item_id
            HAVING SUM(CASE WHEN d_date < '2000-03-11' THEN inv_quantity_on_hand ELSE 0 END) > 0
            ORDER BY w_warehouse_name, i_item_id
            LIMIT 100
            """;

        System.out.println("\n" + "=".repeat(70));
        System.out.println("TPC-DS Q21 (Inventory Analysis - 4-way join)");
        System.out.println("=".repeat(70));
        System.out.println(detector.analyze(q21).format());
    }

    @Test
    void analyzeQ37_JoinOrderProblem() {
        Assumptions.assumeTrue(available);

        // This is the query that takes 1.5h with bad join order (Issue #3525)
        String q37 = """
            SELECT i_item_id, i_item_desc, i_current_price
            FROM item, inventory, date_dim, catalog_sales
            WHERE i_current_price BETWEEN 22 AND 52
              AND inv_item_sk = i_item_sk
              AND d_date_sk = inv_date_sk
              AND d_date BETWEEN '2001-06-02' AND '2001-08-02'
              AND i_manufact_id IN (678, 964, 918, 849)
              AND inv_quantity_on_hand BETWEEN 100 AND 500
              AND cs_item_sk = i_item_sk
            GROUP BY i_item_id, i_item_desc, i_current_price
            ORDER BY i_item_id
            LIMIT 100
            """;

        System.out.println("\n" + "=".repeat(70));
        System.out.println("TPC-DS Q37 (Join Order Problem - Issue #3525)");
        System.out.println("=".repeat(70));
        System.out.println(detector.analyze(q37).format());
    }

    @Test
    void analyzeGroupedTopN() {
        Assumptions.assumeTrue(available);

        String query = """
            SELECT * FROM (
                SELECT c_customer_id, c_first_name, ss_net_paid,
                       ROW_NUMBER() OVER (PARTITION BY c_customer_id ORDER BY ss_net_paid DESC) as rn
                FROM customer
                JOIN store_sales ON c_customer_sk = ss_customer_sk
                WHERE ss_net_paid > 100
            ) ranked
            WHERE rn <= 3
            """;

        System.out.println("\n" + "=".repeat(70));
        System.out.println("Grouped TopN (Top 3 sales per customer)");
        System.out.println("=".repeat(70));
        System.out.println(detector.analyze(query).format());
    }

    @Test
    void analyzeQ17_SemiJoinPattern() {
        Assumptions.assumeTrue(available);

        // Q17-like query with correlated subquery
        String query = """
            SELECT i_item_id, i_item_desc, s_state,
                   COUNT(ss_quantity) as store_sales_cnt,
                   AVG(ss_quantity) as avg_qty
            FROM store_sales, store, item, date_dim
            WHERE ss_sold_date_sk = d_date_sk
              AND ss_item_sk = i_item_sk
              AND ss_store_sk = s_store_sk
              AND d_month_seq IN (1200, 1201, 1202, 1203, 1204, 1205)
              AND EXISTS (
                  SELECT * FROM catalog_sales
                  WHERE cs_item_sk = i_item_sk
                    AND cs_sold_date_sk = d_date_sk
              )
            GROUP BY i_item_id, i_item_desc, s_state
            ORDER BY i_item_id, s_state
            LIMIT 100
            """;

        System.out.println("\n" + "=".repeat(70));
        System.out.println("Q17-like (EXISTS subquery pattern)");
        System.out.println("=".repeat(70));
        System.out.println(detector.analyze(query).format());
    }

    @Test
    void analyzeMultipleLeftJoins() {
        Assumptions.assumeTrue(available);

        String query = """
            SELECT ss.ss_item_sk, d1.d_date, d2.d_date, d3.d_date, d4.d_date
            FROM store_sales ss
            LEFT JOIN date_dim d1 ON ss.ss_sold_date_sk = d1.d_date_sk
            LEFT JOIN date_dim d2 ON ss.ss_sold_date_sk = d2.d_date_sk
            LEFT JOIN date_dim d3 ON ss.ss_sold_date_sk = d3.d_date_sk
            LEFT JOIN date_dim d4 ON ss.ss_sold_date_sk = d4.d_date_sk
            WHERE d1.d_year = 2001
            LIMIT 100
            """;

        System.out.println("\n" + "=".repeat(70));
        System.out.println("Multiple LEFT JOINs (Issue #14354)");
        System.out.println("=".repeat(70));
        System.out.println(detector.analyze(query).format());
    }
}
