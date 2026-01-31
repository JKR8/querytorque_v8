package com.qtcalcite;

import com.qtcalcite.calcite.DuckDBStatistics;
import com.qtcalcite.calcite.RuleRegistry;
import com.qtcalcite.calcite.VolcanoOptimizer;
import com.qtcalcite.config.AppConfig;
import com.qtcalcite.duckdb.DuckDBAdapter;
import com.qtcalcite.llm.DeepSeekClient;
import com.qtcalcite.llm.PromptBuilder;
import com.qtcalcite.llm.ResponseParser;
import com.qtcalcite.llm.VolcanoPromptBuilder;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test that uses DeepSeek LLM to select Volcano optimization rules.
 *
 * Run with: ./gradlew test --tests "com.qtcalcite.VolcanoDeepSeekIntegrationTest"
 *
 * Requires DEEPSEEK_API_KEY environment variable or config/application.yaml
 */
public class VolcanoDeepSeekIntegrationTest {

    private static final String TPCDS_PATH = System.getenv("TPCDS_PATH") != null
            ? System.getenv("TPCDS_PATH")
            : System.getProperty("tpcds.path", "/mnt/d/TPC-DS/tpcds_sf100.duckdb");

    private static DuckDBAdapter adapter;
    private static VolcanoOptimizer optimizer;
    private static RuleRegistry ruleRegistry;
    private static DuckDBStatistics statistics;
    private static DeepSeekClient deepSeekClient;
    private static VolcanoPromptBuilder promptBuilder;
    private static ResponseParser responseParser;

    private static boolean available = false;

    // Test query - 3-way join that benefits from optimization
    private static final String TEST_QUERY = """
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

    // More complex query with multiple filters
    private static final String COMPLEX_QUERY = """
        SELECT c.c_customer_id, c.c_first_name, c.c_last_name,
               d.d_year, SUM(ss.ss_net_paid) as yearly_sales
        FROM store_sales ss
        JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
        JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        JOIN store s ON ss.ss_store_sk = s.s_store_sk
        WHERE d.d_year = 2001
          AND s.s_city = 'Midway'
          AND c.c_birth_year BETWEEN 1960 AND 1980
        GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name, d.d_year
        HAVING SUM(ss.ss_net_paid) > 1000
        ORDER BY yearly_sales DESC
        LIMIT 50
    """;

    @BeforeAll
    static void setup() {
        try {
            // Load TPC-DS database
            adapter = new DuckDBAdapter(TPCDS_PATH);
            optimizer = new VolcanoOptimizer(adapter);
            statistics = optimizer.getStatistics();
            ruleRegistry = new RuleRegistry();

            // Initialize LLM client via AppConfig
            AppConfig config = AppConfig.load(null);
            deepSeekClient = new DeepSeekClient(config);

            if (!deepSeekClient.isConfigured()) {
                System.err.println("DeepSeek API key not available. Set DEEPSEEK_API_KEY or configure in application.yaml");
                available = false;
                return;
            }

            promptBuilder = new VolcanoPromptBuilder(ruleRegistry, adapter, statistics);
            responseParser = new ResponseParser(ruleRegistry);

            available = true;
            System.out.println("Integration test setup complete.");
            System.out.println("TPC-DS database: " + TPCDS_PATH);
            System.out.println("Tables: " + statistics.getTableNames());

        } catch (Exception e) {
            System.err.println("Setup failed: " + e.getMessage());
            e.printStackTrace();
            available = false;
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (adapter != null) {
            adapter.close();
        }
    }

    @Test
    void testDeepSeekRuleSelection() throws Exception {
        Assumptions.assumeTrue(available, "DeepSeek integration not available");

        System.out.println("\n" + "=".repeat(70));
        System.out.println("TEST: DeepSeek Rule Selection for Volcano Optimizer");
        System.out.println("=".repeat(70));

        // Generate prompt
        System.out.println("\n--- Generating Volcano Prompt ---");
        String prompt = promptBuilder.buildCompletePrompt(TEST_QUERY);
        System.out.println(prompt);

        // Call DeepSeek
        System.out.println("\n--- Calling DeepSeek API ---");
        VolcanoPromptBuilder.LLMPrompt volcanoPrompt = promptBuilder.buildLLMPrompt(TEST_QUERY);
        // Convert to PromptBuilder.LLMPrompt for DeepSeekClient
        PromptBuilder.LLMPrompt llmPrompt = new PromptBuilder.LLMPrompt(
            volcanoPrompt.getSystemPrompt(),
            volcanoPrompt.getUserPrompt()
        );
        String response = deepSeekClient.chat(llmPrompt);
        System.out.println("DeepSeek Response: " + response);

        // Parse rules
        ResponseParser.ParseResult parseResult = responseParser.parse(response);
        assertTrue(parseResult.hasRules(), "Should parse rules from response: " + parseResult.getError());

        List<String> rules = parseResult.getRules();
        System.out.println("\nParsed Rules: " + String.join(", ", rules));

        // Run Volcano optimization with selected rules
        System.out.println("\n--- Running Volcano Optimization ---");
        VolcanoOptimizer.OptimizationResult result = optimizer.optimize(TEST_QUERY, rules);

        System.out.println(result.formatSummary());

        assertFalse(result.hasError(), "Optimization should succeed: " + result.getError());
        assertNotNull(result.getOptimizedCost(), "Should have cost estimate");
        assertFalse(result.getOptimizedCost().isInfinite(), "Cost should be finite");

        System.out.println("\n" + "=".repeat(70));
    }

    @Test
    void testComplexQueryOptimization() throws Exception {
        Assumptions.assumeTrue(available, "DeepSeek integration not available");

        System.out.println("\n" + "=".repeat(70));
        System.out.println("TEST: Complex Query Optimization with DeepSeek");
        System.out.println("=".repeat(70));

        // Generate prompt
        VolcanoPromptBuilder.LLMPrompt volcanoPrompt = promptBuilder.buildLLMPrompt(COMPLEX_QUERY);

        System.out.println("\n--- User Prompt (abbreviated) ---");
        System.out.println(volcanoPrompt.getUserPrompt().substring(0, Math.min(2000, volcanoPrompt.getUserPrompt().length())));
        System.out.println("...");

        // Call DeepSeek
        System.out.println("\n--- Calling DeepSeek API ---");
        PromptBuilder.LLMPrompt llmPrompt = new PromptBuilder.LLMPrompt(
            volcanoPrompt.getSystemPrompt(),
            volcanoPrompt.getUserPrompt()
        );
        String response = deepSeekClient.chat(llmPrompt);
        System.out.println("DeepSeek Response: " + response);

        // Parse and apply rules
        ResponseParser.ParseResult parseResult = responseParser.parse(response);
        List<String> rules = parseResult.getRules();
        System.out.println("Parsed Rules: " + String.join(", ", rules));

        // Compare with and without LLM-selected rules
        System.out.println("\n--- Cost Comparison ---");

        VolcanoOptimizer.OptimizationResult baseResult = optimizer.optimize(COMPLEX_QUERY, List.of());
        VolcanoOptimizer.OptimizationResult llmResult = optimizer.optimize(COMPLEX_QUERY, rules);

        System.out.println("Baseline (no extra rules): " +
            (baseResult.hasError() ? baseResult.getError() : baseResult.getOptimizedCost()));
        System.out.println("With LLM-selected rules:   " +
            (llmResult.hasError() ? llmResult.getError() : llmResult.getOptimizedCost()));

        if (!baseResult.hasError() && !llmResult.hasError()) {
            System.out.println("\n--- Optimized SQL ---");
            System.out.println(llmResult.getOptimizedSql());
        }

        System.out.println("\n" + "=".repeat(70));
    }

    @Test
    void testStatisticsInPrompt() throws Exception {
        Assumptions.assumeTrue(available, "DeepSeek integration not available");

        System.out.println("\n" + "=".repeat(70));
        System.out.println("TEST: Verify Statistics are Included in Prompt");
        System.out.println("=".repeat(70));

        String userPrompt = promptBuilder.buildUserPrompt(TEST_QUERY);

        // Verify statistics are included
        assertTrue(userPrompt.contains("Table Statistics"),
            "Prompt should include table statistics section");

        // Check for actual table stats
        if (statistics.getTableNames().contains("store_sales")) {
            long rows = statistics.getRowCount("store_sales");
            System.out.println("store_sales row count: " + rows);
            // The formatted number should appear in the prompt
            assertTrue(rows > 0, "Should have store_sales row count");
        }

        System.out.println("\n--- Statistics in Prompt ---");
        int statsStart = userPrompt.indexOf("## Table Statistics");
        int statsEnd = userPrompt.indexOf("## Execution Plan");
        if (statsStart >= 0 && statsEnd > statsStart) {
            System.out.println(userPrompt.substring(statsStart, statsEnd));
        }

        System.out.println("\n" + "=".repeat(70));
    }
}
