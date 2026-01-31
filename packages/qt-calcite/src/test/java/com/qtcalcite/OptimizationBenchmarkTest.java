package com.qtcalcite;

import com.qtcalcite.calcite.CalciteOptimizer;
import com.qtcalcite.calcite.DuckDBStatistics;
import com.qtcalcite.calcite.VolcanoOptimizer;
import com.qtcalcite.detector.GapDetector;
import com.qtcalcite.detector.GapDetector.*;
import com.qtcalcite.duckdb.DuckDBAdapter;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * End-to-end benchmark: Detect gaps → Apply rules → Measure speedup
 */
public class OptimizationBenchmarkTest {

    private static final String TPCDS_PATH = System.getenv("TPCDS_PATH") != null
            ? System.getenv("TPCDS_PATH")
            : "/mnt/d/TPC-DS/tpcds_sf100.duckdb";

    private static DuckDBAdapter adapter;
    private static DuckDBStatistics stats;
    private static GapDetector detector;
    private static CalciteOptimizer hepOptimizer;
    private static VolcanoOptimizer volcanoOptimizer;
    private static Connection conn;
    private static boolean available = false;

    @BeforeAll
    static void setup() throws Exception {
        try {
            adapter = new DuckDBAdapter(TPCDS_PATH);
            stats = new DuckDBStatistics(adapter);
            detector = new GapDetector(adapter, stats);
            hepOptimizer = new CalciteOptimizer(adapter);
            volcanoOptimizer = new VolcanoOptimizer(adapter);
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
    void benchmarkQ37_JoinOrder() throws Exception {
        Assumptions.assumeTrue(available);

        String sql = """
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

        runBenchmark("TPC-DS Q37 (Join Order)", sql);
    }

    @Test
    void benchmarkGroupedTopN() throws Exception {
        Assumptions.assumeTrue(available);

        String sql = """
            SELECT * FROM (
                SELECT c_customer_id, c_first_name, ss_net_paid,
                       ROW_NUMBER() OVER (PARTITION BY c_customer_id ORDER BY ss_net_paid DESC) as rn
                FROM customer
                JOIN store_sales ON c_customer_sk = ss_customer_sk
                WHERE ss_net_paid > 100
            ) ranked
            WHERE rn <= 3
            """;

        runBenchmark("Grouped TopN (Top 3 per customer)", sql);
    }

    @Test
    void benchmarkMultipleLeftJoins() throws Exception {
        Assumptions.assumeTrue(available);

        String sql = """
            SELECT ss.ss_item_sk, ss.ss_net_paid, d1.d_date, d2.d_month_seq
            FROM store_sales ss
            LEFT JOIN date_dim d1 ON ss.ss_sold_date_sk = d1.d_date_sk
            LEFT JOIN date_dim d2 ON ss.ss_sold_date_sk = d2.d_date_sk
            LEFT JOIN date_dim d3 ON ss.ss_sold_date_sk = d3.d_date_sk
            WHERE d1.d_year = 2001
            LIMIT 1000
            """;

        runBenchmark("Multiple LEFT JOINs", sql);
    }

    @Test
    void benchmarkSimpleJoinReorder() throws Exception {
        Assumptions.assumeTrue(available);

        // Smaller query to test join reordering effect
        String sql = """
            SELECT c.c_customer_id, s.s_store_name, SUM(ss.ss_net_paid)
            FROM store_sales ss, customer c, store s, date_dim d
            WHERE ss.ss_customer_sk = c.c_customer_sk
              AND ss.ss_store_sk = s.s_store_sk
              AND ss.ss_sold_date_sk = d.d_date_sk
              AND d.d_year = 2001
              AND s.s_state = 'TN'
            GROUP BY c.c_customer_id, s.s_store_name
            ORDER BY SUM(ss.ss_net_paid) DESC
            LIMIT 100
            """;

        runBenchmark("4-way Join with Filters", sql);
    }

    private void runBenchmark(String name, String sql) throws Exception {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("BENCHMARK: " + name);
        System.out.println("=".repeat(70));

        // Step 1: Detect gaps
        GapAnalysis analysis = detector.analyze(sql);
        System.out.println("\n--- Gap Detection ---");
        if (analysis.hasGaps()) {
            for (DetectedGap gap : analysis.getGaps()) {
                System.out.println("  Gap: " + gap.getGap().getDisplayName());
                System.out.println("  Rules: " + String.join(", ", gap.getRecommendedRules()));
            }
        } else {
            System.out.println("  No gaps detected");
        }

        // Step 2: Apply optimization
        String optimizedSql = null;
        List<String> rules = analysis.getAllRecommendedRules();

        if (!rules.isEmpty()) {
            System.out.println("\n--- Applying Rules ---");
            System.out.println("  Optimizer: " + analysis.getRecommendedOptimizer());
            System.out.println("  Rules: " + String.join(", ", rules));

            try {
                if (analysis.getRecommendedOptimizer() == OptimizerType.VOLCANO) {
                    VolcanoOptimizer.OptimizationResult result = volcanoOptimizer.optimize(sql, rules);
                    if (!result.hasError()) {
                        optimizedSql = cleanSql(result.getOptimizedSql());
                        System.out.println("  Cost reduction: " +
                            String.format("%.2e → %.2e", result.getOriginalCost(), result.getOptimizedCost()));
                    } else {
                        System.out.println("  Volcano error: " + result.getError());
                    }
                } else {
                    CalciteOptimizer.OptimizationResult result = hepOptimizer.optimize(sql, rules);
                    if (result.isChanged()) {
                        optimizedSql = cleanSql(result.getOptimizedSql());
                    }
                }
            } catch (Exception e) {
                System.out.println("  Optimization error: " + e.getMessage());
            }
        }

        // Step 3: Benchmark execution
        System.out.println("\n--- Execution Benchmark ---");

        // Warmup
        System.out.println("  Warming up cache...");
        for (int i = 0; i < 2; i++) {
            executeQuery(sql);
            if (optimizedSql != null) {
                executeQuery(optimizedSql);
            }
        }

        // Timed runs
        List<Long> originalTimes = new ArrayList<>();
        List<Long> optimizedTimes = new ArrayList<>();

        System.out.println("  Running 3 iterations...");
        for (int i = 0; i < 3; i++) {
            originalTimes.add(executeQuery(sql));
            if (optimizedSql != null) {
                optimizedTimes.add(executeQuery(optimizedSql));
            }
        }

        // Results
        long origAvg = average(originalTimes);
        long origMin = min(originalTimes);

        System.out.println("\n--- Results ---");
        System.out.println("  Original:  avg=" + origAvg + "ms, min=" + origMin + "ms " + originalTimes);

        if (optimizedSql != null && !optimizedTimes.isEmpty()) {
            long optAvg = average(optimizedTimes);
            long optMin = min(optimizedTimes);
            System.out.println("  Optimized: avg=" + optAvg + "ms, min=" + optMin + "ms " + optimizedTimes);

            double speedup = (double) origAvg / optAvg;
            String result = speedup > 1.0 ? "FASTER" : (speedup < 1.0 ? "SLOWER" : "SAME");
            System.out.println("  Speedup: " + String.format("%.2fx", speedup) + " (" + result + ")");

            if (speedup < 0.9) {
                System.out.println("\n  Optimized SQL:");
                System.out.println("  " + optimizedSql.replace("\n", "\n  "));
            }
        } else {
            System.out.println("  Optimized: N/A (no optimization applied)");
        }

        System.out.println("=".repeat(70));
    }

    private long executeQuery(String sql) throws SQLException {
        long start = System.currentTimeMillis();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) count++;
        }
        return System.currentTimeMillis() - start;
    }

    private String cleanSql(String sql) {
        return sql
            .replaceAll("`?DUCKDB`?\\.", "")
            .replace("`", "\"")
            .replaceAll("FETCH NEXT (\\d+) ROWS ONLY", "LIMIT $1");
    }

    private long average(List<Long> times) {
        return times.stream().mapToLong(Long::longValue).sum() / times.size();
    }

    private long min(List<Long> times) {
        return times.stream().mapToLong(Long::longValue).min().orElse(0);
    }
}
