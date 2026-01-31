package com.qtcalcite;

import com.qtcalcite.calcite.RuleRegistry;
import com.qtcalcite.calcite.VolcanoOptimizer;
import com.qtcalcite.config.AppConfig;
import com.qtcalcite.duckdb.DuckDBAdapter;
import com.qtcalcite.llm.DeepSeekClient;
import com.qtcalcite.llm.PromptBuilder;
import com.qtcalcite.llm.ResponseParser;
import com.qtcalcite.llm.VolcanoPromptBuilder;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Benchmark test that measures actual execution times, not optimizer cost estimates.
 */
public class VolcanoBenchmarkTest {

    private static final String TPCDS_PATH = "/mnt/d/TPC-DS/tpcds_sf100.duckdb";
    private static DuckDBAdapter adapter;
    private static VolcanoOptimizer optimizer;
    private static Connection conn;
    private static boolean available = false;

    // Q4-style query: customer sales comparison across years
    private static final String Q4_STYLE_QUERY = """
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

    // Simpler 3-way join for testing
    private static final String THREE_WAY_JOIN = """
        SELECT c.c_customer_id, s.s_store_name, SUM(ss.ss_net_paid) as total_sales
        FROM store_sales ss
        JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
        JOIN store s ON ss.ss_store_sk = s.s_store_sk
        WHERE s.s_city = 'Midway'
          AND c.c_birth_year > 1970
        GROUP BY c.c_customer_id, s.s_store_name
        ORDER BY total_sales DESC
        LIMIT 100
    """;

    @BeforeAll
    static void setup() throws Exception {
        try {
            adapter = new DuckDBAdapter(TPCDS_PATH);
            optimizer = new VolcanoOptimizer(adapter);
            conn = adapter.getConnection();
            available = true;
            System.out.println("Connected to TPC-DS database: " + TPCDS_PATH);
        } catch (Exception e) {
            System.err.println("TPC-DS not available: " + e.getMessage());
            available = false;
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (adapter != null) adapter.close();
    }

    @Test
    void benchmarkQ4StyleQuery() throws Exception {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        System.out.println("\n" + "=".repeat(70));
        System.out.println("BENCHMARK: Q4-Style Query (Customer Year-over-Year Comparison)");
        System.out.println("=".repeat(70));

        // Get optimized SQL from Volcano
        List<String> rules = List.of(
            "FILTER_INTO_JOIN",
            "FILTER_PROJECT_TRANSPOSE",
            "PROJECT_MERGE",
            "JOIN_COMMUTE"
        );

        VolcanoOptimizer.OptimizationResult result = optimizer.optimize(Q4_STYLE_QUERY, rules);

        if (result.hasError()) {
            System.out.println("Optimization error: " + result.getError());
            // Fall back to original
            benchmarkQuery("Q4-Style", Q4_STYLE_QUERY, Q4_STYLE_QUERY);
        } else {
            benchmarkQuery("Q4-Style", Q4_STYLE_QUERY, result.getOptimizedSql());
        }
    }

    @Test
    void benchmarkThreeWayJoin() throws Exception {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        System.out.println("\n" + "=".repeat(70));
        System.out.println("BENCHMARK: 3-Way Join Query");
        System.out.println("=".repeat(70));

        // Get optimized SQL from Volcano
        List<String> rules = List.of(
            "FILTER_INTO_JOIN",
            "FILTER_PROJECT_TRANSPOSE",
            "PROJECT_MERGE"
        );

        VolcanoOptimizer.OptimizationResult result = optimizer.optimize(THREE_WAY_JOIN, rules);

        if (result.hasError()) {
            System.out.println("Optimization error: " + result.getError());
            benchmarkQuery("3-Way Join", THREE_WAY_JOIN, THREE_WAY_JOIN);
        } else {
            benchmarkQuery("3-Way Join", THREE_WAY_JOIN, result.getOptimizedSql());
        }
    }

    @Test
    void benchmarkWithDeepSeekRules() throws Exception {
        Assumptions.assumeTrue(available, "TPC-DS not available");

        System.out.println("\n" + "=".repeat(70));
        System.out.println("BENCHMARK: DeepSeek-Selected Rules");
        System.out.println("=".repeat(70));

        // Initialize DeepSeek
        AppConfig config = AppConfig.load(null);
        DeepSeekClient client = new DeepSeekClient(config);

        if (!client.isConfigured()) {
            System.out.println("DeepSeek not configured, skipping");
            return;
        }

        RuleRegistry ruleRegistry = new RuleRegistry();
        VolcanoPromptBuilder promptBuilder = new VolcanoPromptBuilder(
            ruleRegistry, adapter, optimizer.getStatistics()
        );
        ResponseParser responseParser = new ResponseParser(ruleRegistry);

        // Get rules from DeepSeek
        VolcanoPromptBuilder.LLMPrompt volcanoPrompt = promptBuilder.buildLLMPrompt(THREE_WAY_JOIN);
        PromptBuilder.LLMPrompt llmPrompt = new PromptBuilder.LLMPrompt(
            volcanoPrompt.getSystemPrompt(),
            volcanoPrompt.getUserPrompt()
        );

        System.out.println("Calling DeepSeek for rule selection...");
        String response = client.chat(llmPrompt);
        System.out.println("DeepSeek response: " + response);

        ResponseParser.ParseResult parseResult = responseParser.parse(response);
        List<String> rules = parseResult.getRules();
        System.out.println("Parsed rules: " + rules);

        // Optimize with DeepSeek rules
        VolcanoOptimizer.OptimizationResult result = optimizer.optimize(THREE_WAY_JOIN, rules);

        if (result.hasError()) {
            System.out.println("Optimization error: " + result.getError());
        } else {
            benchmarkQuery("DeepSeek-Optimized", THREE_WAY_JOIN, result.getOptimizedSql());
        }
    }

    private void benchmarkQuery(String name, String originalSql, String optimizedSql) throws Exception {
        // Clean up Calcite-generated SQL for DuckDB compatibility
        String cleanedOptimizedSql = cleanSqlForDuckDB(optimizedSql);

        System.out.println("\n--- Original Query ---");
        System.out.println(originalSql.trim());

        System.out.println("\n--- Optimized Query (cleaned) ---");
        System.out.println(cleanedOptimizedSql.trim());

        // Use cleaned SQL
        optimizedSql = cleanedOptimizedSql;

        // Warmup runs
        System.out.println("\n--- Warming up cache (3 runs each) ---");
        for (int i = 0; i < 3; i++) {
            executeAndTime(originalSql, true);
            executeAndTime(optimizedSql, true);
        }

        // Timed runs
        System.out.println("\n--- Timed runs (5 iterations) ---");
        List<Long> originalTimes = new ArrayList<>();
        List<Long> optimizedTimes = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            originalTimes.add(executeAndTime(originalSql, false));
            optimizedTimes.add(executeAndTime(optimizedSql, false));
        }

        // Results
        long origAvg = originalTimes.stream().mapToLong(Long::longValue).sum() / originalTimes.size();
        long optAvg = optimizedTimes.stream().mapToLong(Long::longValue).sum() / optimizedTimes.size();
        long origMin = originalTimes.stream().mapToLong(Long::longValue).min().orElse(0);
        long optMin = optimizedTimes.stream().mapToLong(Long::longValue).min().orElse(0);

        System.out.println("\n" + "=".repeat(50));
        System.out.println("RESULTS: " + name);
        System.out.println("=".repeat(50));
        System.out.printf("Original:  avg=%dms, min=%dms, times=%s%n", origAvg, origMin, originalTimes);
        System.out.printf("Optimized: avg=%dms, min=%dms, times=%s%n", optAvg, optMin, optimizedTimes);

        if (origAvg > 0) {
            double speedup = (double) origAvg / optAvg;
            double pctChange = ((double)(origAvg - optAvg) / origAvg) * 100;
            System.out.printf("Speedup: %.2fx (%.1f%% %s)%n",
                speedup,
                Math.abs(pctChange),
                pctChange > 0 ? "faster" : "slower");
        }
        System.out.println("=".repeat(50));
    }

    /**
     * Clean Calcite-generated SQL for DuckDB execution.
     */
    private String cleanSqlForDuckDB(String sql) {
        return sql
            // Remove schema prefix
            .replaceAll("`?DUCKDB`?\\.", "")
            // Remove backticks (DuckDB uses double quotes)
            .replace("`", "\"")
            // FETCH NEXT N ROWS ONLY -> LIMIT N
            .replaceAll("FETCH NEXT (\\d+) ROWS ONLY", "LIMIT $1");
    }

    private long executeAndTime(String sql, boolean warmup) throws Exception {
        long start = System.currentTimeMillis();
        int rowCount = 0;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                rowCount++;
            }
        }

        long elapsed = System.currentTimeMillis() - start;
        if (!warmup) {
            System.out.printf("  %dms (%d rows)%n", elapsed, rowCount);
        }
        return elapsed;
    }
}
