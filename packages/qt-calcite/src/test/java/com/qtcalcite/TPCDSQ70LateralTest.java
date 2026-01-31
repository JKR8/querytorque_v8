package com.qtcalcite;

import com.qtcalcite.duckdb.DuckDBAdapter;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

/**
 * Test LATERAL rewrite on TPC-DS Q70 pattern.
 *
 * Q70 has: RANK() OVER (PARTITION BY s_state ORDER BY sum(...) DESC) with WHERE ranking <= 5
 * This is the grouped TopN pattern that benefits from LATERAL.
 */
public class TPCDSQ70LateralTest {

    private static final String TPCDS_PATH = System.getenv("TPCDS_PATH") != null
            ? System.getenv("TPCDS_PATH")
            : "/mnt/d/TPC-DS/tpcds_sf100.duckdb";

    private static DuckDBAdapter adapter;
    private static Connection conn;
    private static boolean available = false;

    @BeforeAll
    static void setup() throws Exception {
        try {
            adapter = new DuckDBAdapter(TPCDS_PATH);
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
    void testQ70Pattern_TopStoresPerState() throws Exception {
        Assumptions.assumeTrue(available);

        // Q70 inner pattern: Top 5 stores by profit per state
        // Window version (original TPC-DS pattern)
        String windowVersion = """
            SELECT s_state, s_store_id, total_profit, ranking FROM (
                SELECT s_state,
                       s_store_id,
                       SUM(ss_net_profit) as total_profit,
                       RANK() OVER (PARTITION BY s_state ORDER BY SUM(ss_net_profit) DESC) as ranking
                FROM store_sales
                JOIN store ON s_store_sk = ss_store_sk
                JOIN date_dim ON d_date_sk = ss_sold_date_sk
                WHERE d_month_seq BETWEEN 1176 AND 1187
                GROUP BY s_state, s_store_id
            ) ranked
            WHERE ranking <= 5
            """;

        // LATERAL version - iterate over states, get top 5 stores per state
        String lateralVersion = """
            SELECT states.s_state, t.s_store_id, t.total_profit
            FROM (SELECT DISTINCT s_state FROM store) states,
            LATERAL (
                SELECT s.s_store_id, SUM(ss_net_profit) as total_profit
                FROM store_sales ss
                JOIN store s ON s.s_store_sk = ss.ss_store_sk
                JOIN date_dim d ON d.d_date_sk = ss.ss_sold_date_sk
                WHERE s.s_state = states.s_state
                  AND d.d_month_seq BETWEEN 1176 AND 1187
                GROUP BY s.s_store_id
                ORDER BY total_profit DESC
                LIMIT 5
            ) t
            """;

        System.out.println("=".repeat(70));
        System.out.println("TPC-DS Q70 PATTERN: Top 5 Stores per State");
        System.out.println("=".repeat(70));

        // Get state count
        int stateCount = countRows("SELECT COUNT(DISTINCT s_state) FROM store");
        System.out.println("\nDistinct states: " + stateCount + " (small NDV = LATERAL should win)");

        // Show plans
        System.out.println("\n--- WINDOW VERSION PLAN ---");
        printPlanSummary(adapter.getExplainPlan(windowVersion));

        System.out.println("\n--- LATERAL VERSION PLAN ---");
        printPlanSummary(adapter.getExplainPlan(lateralVersion));

        // Benchmark
        System.out.println("\n--- BENCHMARK ---");
        runBenchmark(windowVersion, lateralVersion);
    }

    @Test
    void testSimpleTopNPerState() throws Exception {
        Assumptions.assumeTrue(available);

        // Simpler version without aggregation in window
        String windowVersion = """
            SELECT * FROM (
                SELECT s_state, s_store_id, ss_net_profit,
                       ROW_NUMBER() OVER (PARTITION BY s_state ORDER BY ss_net_profit DESC) as rn
                FROM store
                JOIN store_sales ON s_store_sk = ss_store_sk
                JOIN date_dim ON d_date_sk = ss_sold_date_sk
                WHERE d_year = 2001 AND d_moy <= 3
            ) ranked
            WHERE rn <= 5
            """;

        String lateralVersion = """
            SELECT states.s_state, t.s_store_id, t.ss_net_profit
            FROM (SELECT DISTINCT s_state FROM store) states,
            LATERAL (
                SELECT s.s_store_id, ss.ss_net_profit
                FROM store_sales ss
                JOIN store s ON s.s_store_sk = ss.ss_store_sk
                JOIN date_dim d ON d.d_date_sk = ss.ss_sold_date_sk
                WHERE s.s_state = states.s_state
                  AND d.d_year = 2001 AND d.d_moy <= 3
                ORDER BY ss.ss_net_profit DESC
                LIMIT 5
            ) t
            """;

        System.out.println("=".repeat(70));
        System.out.println("SIMPLE TOP-N PER STATE");
        System.out.println("=".repeat(70));

        int stateCount = countRows("SELECT COUNT(DISTINCT s_state) FROM store");
        System.out.println("\nDistinct states: " + stateCount);

        runBenchmark(windowVersion, lateralVersion);
    }

    private void printPlanSummary(String plan) {
        String[] lines = plan.split("\n");
        Set<String> operators = new LinkedHashSet<>();
        for (String line : lines) {
            line = line.trim();
            if (line.contains("TOP_N")) operators.add("TOP_N");
            if (line.contains("WINDOW")) operators.add("WINDOW");
            if (line.contains("HASH_JOIN")) operators.add("HASH_JOIN");
            if (line.contains("NESTED")) operators.add("NESTED_LOOP");
            if (line.contains("FILTER")) operators.add("FILTER");
            if (line.contains("HASH_GROUP")) operators.add("HASH_GROUP_BY");
            if (line.contains("SEQ_SCAN")) operators.add("SEQ_SCAN");
        }
        System.out.println("Operators: " + operators);
    }

    private void runBenchmark(String original, String optimized) throws Exception {
        // Warmup
        System.out.println("Warming up...");
        for (int i = 0; i < 2; i++) {
            runQuery(original);
            runQuery(optimized);
        }

        // Timed runs
        List<Long> originalTimes = new ArrayList<>();
        List<Long> optimizedTimes = new ArrayList<>();

        System.out.println("Running 3 iterations...");
        for (int i = 0; i < 3; i++) {
            originalTimes.add(runQuery(original));
            optimizedTimes.add(runQuery(optimized));
        }

        long origAvg = avg(originalTimes);
        long optAvg = avg(optimizedTimes);

        System.out.println("\n--- Results ---");
        System.out.println("Window:  avg=" + origAvg + "ms " + originalTimes);
        System.out.println("LATERAL: avg=" + optAvg + "ms " + optimizedTimes);

        double speedup = (double) origAvg / optAvg;
        String result = speedup > 1.05 ? "LATERAL FASTER" : (speedup < 0.95 ? "WINDOW FASTER" : "SAME");
        System.out.println("Speedup: " + String.format("%.2fx", speedup) + " (" + result + ")");

        if (speedup > 1.5) {
            System.out.println("\n*** SIGNIFICANT IMPROVEMENT: " +
                String.format("%.0f%%", (speedup - 1) * 100) + " faster ***");
        }
        System.out.println("=".repeat(70));
    }

    private long runQuery(String sql) throws SQLException {
        long start = System.currentTimeMillis();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
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
