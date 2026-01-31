package com.qtcalcite;

import com.qtcalcite.duckdb.DuckDBAdapter;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

/**
 * Test QUALIFY rewrite - this is a semantic rewrite DuckDB can't do automatically.
 *
 * Original: SELECT * FROM (SELECT ..., ROW_NUMBER() OVER (...) as rn FROM ...) WHERE rn <= 3
 * Rewrite:  SELECT ... FROM ... QUALIFY ROW_NUMBER() OVER (...) <= 3
 */
public class QualifyRewriteTest {

    private static final String TPCDS_PATH = "/mnt/d/TPC-DS/tpcds_sf100.duckdb";
    private static DuckDBAdapter adapter;
    private static Connection conn;

    @BeforeAll
    static void setup() throws Exception {
        adapter = new DuckDBAdapter(TPCDS_PATH);
        conn = adapter.getConnection();
    }

    @AfterAll
    static void teardown() throws Exception {
        if (adapter != null) adapter.close();
    }

    @Test
    void benchmarkGroupedTopN_SubqueryVsQualify() throws Exception {
        // Original pattern - subquery with ROW_NUMBER filter (limited to 1 year for memory)
        String subqueryVersion = """
            SELECT customer_id, first_name, net_paid, rn FROM (
                SELECT c_customer_id as customer_id,
                       c_first_name as first_name,
                       ss_net_paid as net_paid,
                       ROW_NUMBER() OVER (PARTITION BY c_customer_id ORDER BY ss_net_paid DESC) as rn
                FROM customer
                JOIN store_sales ON c_customer_sk = ss_customer_sk
                JOIN date_dim ON ss_sold_date_sk = d_date_sk
                WHERE d_year = 2001 AND ss_net_paid > 100
            ) ranked
            WHERE rn <= 3
            """;

        // QUALIFY rewrite - DuckDB can optimize this better
        String qualifyVersion = """
            SELECT c_customer_id as customer_id,
                   c_first_name as first_name,
                   ss_net_paid as net_paid,
                   ROW_NUMBER() OVER (PARTITION BY c_customer_id ORDER BY ss_net_paid DESC) as rn
            FROM customer
            JOIN store_sales ON c_customer_sk = ss_customer_sk
            JOIN date_dim ON ss_sold_date_sk = d_date_sk
            WHERE d_year = 2001 AND ss_net_paid > 100
            QUALIFY ROW_NUMBER() OVER (PARTITION BY c_customer_id ORDER BY ss_net_paid DESC) <= 3
            """;

        System.out.println("=".repeat(70));
        System.out.println("GROUPED TOPN: Subquery vs QUALIFY");
        System.out.println("=".repeat(70));

        // Show plans
        System.out.println("\n--- SUBQUERY VERSION EXPLAIN ---");
        String subqueryPlan = adapter.getExplainPlan(subqueryVersion);
        printPlanSummary(subqueryPlan);

        System.out.println("\n--- QUALIFY VERSION EXPLAIN ---");
        String qualifyPlan = adapter.getExplainPlan(qualifyVersion);
        printPlanSummary(qualifyPlan);

        // Benchmark
        System.out.println("\n--- BENCHMARK ---");

        // Warmup
        System.out.println("Warming up...");
        for (int i = 0; i < 2; i++) {
            runQuery(subqueryVersion);
            runQuery(qualifyVersion);
        }

        // Timed runs
        List<Long> subqueryTimes = new ArrayList<>();
        List<Long> qualifyTimes = new ArrayList<>();

        System.out.println("Running 3 iterations...");
        for (int i = 0; i < 3; i++) {
            subqueryTimes.add(runQuery(subqueryVersion));
            qualifyTimes.add(runQuery(qualifyVersion));
        }

        long subqueryAvg = avg(subqueryTimes);
        long qualifyAvg = avg(qualifyTimes);

        System.out.println("\n--- RESULTS ---");
        System.out.println("Subquery: avg=" + subqueryAvg + "ms, times=" + subqueryTimes);
        System.out.println("QUALIFY:  avg=" + qualifyAvg + "ms, times=" + qualifyTimes);

        double speedup = (double) subqueryAvg / qualifyAvg;
        System.out.println("Speedup:  " + String.format("%.2fx", speedup) +
            (speedup > 1 ? " (QUALIFY FASTER)" : " (SUBQUERY FASTER)"));
        System.out.println("=".repeat(70));
    }

    @Test
    void benchmarkLateralJoin_TopNPerGroup() throws Exception {
        // For small number of groups, LATERAL can be faster than window

        // Window version - top 5 sales per store for Q1 2001
        String windowVersion = """
            SELECT * FROM (
                SELECT s_store_id, s_store_name, d_month_seq, ss_net_paid,
                       ROW_NUMBER() OVER (PARTITION BY s_store_id ORDER BY ss_net_paid DESC) as rn
                FROM store
                JOIN store_sales ON s_store_sk = ss_store_sk
                JOIN date_dim ON ss_sold_date_sk = d_date_sk
                WHERE d_year = 2001 AND d_moy <= 3
            ) ranked
            WHERE rn <= 5
            """;

        // LATERAL version - join each store to its top 5 sales
        String lateralVersion = """
            SELECT s.s_store_id, s.s_store_name, t.d_month_seq, t.ss_net_paid
            FROM store s,
            LATERAL (
                SELECT d.d_month_seq, ss.ss_net_paid
                FROM store_sales ss
                JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
                WHERE ss.ss_store_sk = s.s_store_sk
                  AND d.d_year = 2001 AND d.d_moy <= 3
                ORDER BY ss_net_paid DESC
                LIMIT 5
            ) t
            """;

        System.out.println("=".repeat(70));
        System.out.println("TOP-N PER STORE: Window vs LATERAL");
        System.out.println("Number of stores: " + countRows("SELECT COUNT(*) FROM store"));
        System.out.println("=".repeat(70));

        // Show plans
        System.out.println("\n--- WINDOW VERSION EXPLAIN ---");
        printPlanSummary(adapter.getExplainPlan(windowVersion));

        System.out.println("\n--- LATERAL VERSION EXPLAIN ---");
        printPlanSummary(adapter.getExplainPlan(lateralVersion));

        // Benchmark
        System.out.println("\n--- BENCHMARK ---");
        System.out.println("Warming up...");
        for (int i = 0; i < 2; i++) {
            runQuery(windowVersion);
            runQuery(lateralVersion);
        }

        List<Long> windowTimes = new ArrayList<>();
        List<Long> lateralTimes = new ArrayList<>();

        System.out.println("Running 3 iterations...");
        for (int i = 0; i < 3; i++) {
            windowTimes.add(runQuery(windowVersion));
            lateralTimes.add(runQuery(lateralVersion));
        }

        long windowAvg = avg(windowTimes);
        long lateralAvg = avg(lateralTimes);

        System.out.println("\n--- RESULTS ---");
        System.out.println("Window:  avg=" + windowAvg + "ms, times=" + windowTimes);
        System.out.println("LATERAL: avg=" + lateralAvg + "ms, times=" + lateralTimes);

        double speedup = (double) windowAvg / lateralAvg;
        System.out.println("Speedup: " + String.format("%.2fx", speedup) +
            (speedup > 1 ? " (LATERAL FASTER)" : " (WINDOW FASTER)"));
        System.out.println("=".repeat(70));
    }

    private void printPlanSummary(String plan) {
        // Extract key operators
        String[] lines = plan.split("\n");
        Set<String> operators = new LinkedHashSet<>();
        for (String line : lines) {
            line = line.trim();
            if (line.contains("TOP_N")) operators.add("TOP_N");
            if (line.contains("WINDOW")) operators.add("WINDOW");
            if (line.contains("HASH_JOIN")) operators.add("HASH_JOIN");
            if (line.contains("NESTED")) operators.add("NESTED_LOOP");
            if (line.contains("SEQ_SCAN")) operators.add("SEQ_SCAN");
            if (line.contains("FILTER")) operators.add("FILTER");
            if (line.contains("HASH_GROUP")) operators.add("HASH_GROUP_BY");
        }
        System.out.println("Operators: " + operators);
    }

    private long runQuery(String sql) throws SQLException {
        long start = System.currentTimeMillis();
        int count = 0;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) count++;
        }
        return System.currentTimeMillis() - start;
    }

    private int countRows(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private long avg(List<Long> times) {
        return times.stream().mapToLong(Long::longValue).sum() / times.size();
    }
}
