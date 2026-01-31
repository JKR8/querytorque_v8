package com.qtcalcite.cli;

import com.qtcalcite.calcite.CalciteOptimizer;
import com.qtcalcite.calcite.RuleRegistry;
import com.qtcalcite.calcite.VolcanoOptimizer;
import com.qtcalcite.config.AppConfig;
import com.qtcalcite.duckdb.DuckDBAdapter;
import com.qtcalcite.duckdb.ResultValidator;
import com.qtcalcite.llm.DeepSeekClient;
import com.qtcalcite.llm.PromptBuilder;
import com.qtcalcite.llm.ResponseParser;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "auto",
        description = "Auto mode: automatically calls DeepSeek API for rule selection"
)
public class AutoCommand implements Callable<Integer> {

    @ParentCommand
    private MainCommand parent;

    @Parameters(index = "0", description = "SQL query or @filename for file input")
    private String queryInput;

    @Option(names = {"--dry-run"}, description = "Show optimized SQL without executing")
    private boolean dryRun;

    @Option(names = {"--compare"}, description = "Run both original and optimized, show timing")
    private boolean compare;

    @Option(names = {"--explain"}, description = "Show the explain plan used for optimization")
    private boolean showExplain;

    @Option(names = {"--show-prompt"}, description = "Show the prompt sent to LLM")
    private boolean showPrompt;

    @Option(names = {"--volcano"}, description = "Use Volcano cost-based planner instead of Hep")
    private boolean useVolcano;

    @Override
    public Integer call() {
        try {
            // Load configuration
            AppConfig config = AppConfig.load(parent.getConfigPath());
            if (parent.getDatabasePath() != null) {
                config.setDatabasePath(parent.getDatabasePath());
            }
            if (parent.isVerbose()) {
                config.setVerbose(true);
            }

            // Get SQL query
            String sql = resolveQuery(queryInput);
            if (sql == null || sql.trim().isEmpty()) {
                System.err.println("Error: No SQL query provided");
                return 1;
            }

            // Check API key
            DeepSeekClient client = new DeepSeekClient(config);
            if (!client.isConfigured()) {
                System.err.println("Error: DeepSeek API key not configured.");
                System.err.println("Set the DEEPSEEK_API_KEY environment variable or configure it in application.yaml");
                return 1;
            }

            // Connect to DuckDB
            try (DuckDBAdapter duckDB = new DuckDBAdapter(config.getDatabasePath())) {
                // Initialize components
                RuleRegistry ruleRegistry = new RuleRegistry();
                PromptBuilder promptBuilder = new PromptBuilder(ruleRegistry, duckDB);

                // Show explain plan if requested
                if (showExplain) {
                    System.out.println("=".repeat(60));
                    System.out.println("EXPLAIN PLAN");
                    System.out.println("=".repeat(60));
                    System.out.println(duckDB.getExplainPlan(sql));
                    System.out.println();
                }

                // Build prompt
                PromptBuilder.LLMPrompt prompt = promptBuilder.buildLLMPrompt(sql);

                if (showPrompt) {
                    System.out.println("=".repeat(60));
                    System.out.println("LLM PROMPT");
                    System.out.println("=".repeat(60));
                    System.out.println("--- System ---");
                    System.out.println(prompt.getSystemPrompt());
                    System.out.println("\n--- User ---");
                    System.out.println(prompt.getUserPrompt());
                    System.out.println();
                }

                // Call DeepSeek API
                System.out.println("Calling DeepSeek API for rule selection...");
                String response;
                try {
                    response = client.chat(prompt);
                } catch (DeepSeekClient.ApiException e) {
                    System.err.println("API Error: " + e.getMessage());
                    return 1;
                } catch (IOException e) {
                    System.err.println("Network Error: " + e.getMessage());
                    return 1;
                }

                System.out.println("LLM Response: " + response);

                // Parse rules from response
                ResponseParser parser = new ResponseParser(ruleRegistry);
                ResponseParser.ParseResult parseResult = parser.parse(response);

                if (!parseResult.hasRules()) {
                    System.err.println("Error: " + parseResult.getError());
                    System.err.println("\nThe LLM did not return valid rule names.");
                    System.err.println("Available rules:");
                    for (String ruleName : ruleRegistry.getAvailableRuleNames()) {
                        System.err.println("  - " + ruleName);
                    }
                    return 1;
                }

                List<String> rules = parseResult.getRules();
                System.out.println("Selected rules: " + String.join(", ", rules));

                // Apply optimization
                String optimizedSql;
                boolean queryChanged;

                if (useVolcano) {
                    System.out.println("Using Volcano cost-based planner...");
                    VolcanoOptimizer optimizer = new VolcanoOptimizer(duckDB);
                    VolcanoOptimizer.OptimizationResult result = optimizer.optimize(sql, rules);
                    optimizedSql = result.getOptimizedSql();
                    queryChanged = result.isQueryChanged();

                    System.out.println("\n" + "=".repeat(60));
                    System.out.println("OPTIMIZATION RESULT (Volcano)");
                    System.out.println("=".repeat(60));
                    System.out.println(result.formatSummary());
                } else {
                    CalciteOptimizer optimizer = new CalciteOptimizer(duckDB);
                    CalciteOptimizer.OptimizationResult result = optimizer.optimize(sql, rules);
                    optimizedSql = result.getOptimizedSql();
                    queryChanged = result.isChanged();

                    System.out.println("\n" + "=".repeat(60));
                    System.out.println("OPTIMIZATION RESULT");
                    System.out.println("=".repeat(60));
                    System.out.println(result.formatSummary());
                }

                if (dryRun) {
                    System.out.println("(Dry run - not executing query)");
                    return 0;
                }

                // Execute queries
                if (compare) {
                    executeComparison(duckDB, sql, optimizedSql);
                } else {
                    executeOptimized(duckDB, optimizedSql);
                }

                return 0;

            } catch (SQLException e) {
                System.err.println("Database error: " + e.getMessage());
                if (parent.isVerbose()) {
                    e.printStackTrace();
                }
                return 1;
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            if (parent.isVerbose()) {
                e.printStackTrace();
            }
            return 1;
        }
    }

    private String resolveQuery(String input) throws IOException {
        if (input.startsWith("@")) {
            // Read from file
            Path path = Path.of(input.substring(1));
            if (!Files.exists(path)) {
                throw new IOException("Query file not found: " + path);
            }
            return Files.readString(path);
        }
        return input;
    }

    private void executeComparison(DuckDBAdapter duckDB, String originalSql, String optimizedSql) throws SQLException {
        System.out.println("\n" + "-".repeat(60));
        System.out.println("EXECUTION COMPARISON (with cache warmup)");
        System.out.println("-".repeat(60));

        // Warmup: run original query first to warm cache, discard result timing
        System.out.println("\nWarming up original query...");
        duckDB.executeQuery(originalSql);

        // Timed: run original query
        System.out.println("Executing original query (timed)...");
        DuckDBAdapter.QueryResult originalResult = duckDB.executeQuery(originalSql);

        // Warmup: run optimized query to warm its specific cache paths
        System.out.println("Warming up optimized query...");
        duckDB.executeQuery(optimizedSql);

        // Timed: run optimized query
        System.out.println("Executing optimized query (timed)...");
        DuckDBAdapter.QueryResult optimizedResult = duckDB.executeQuery(optimizedSql);

        // Show results
        System.out.println("\nResults (optimized query):");
        System.out.println(optimizedResult.formatResults(10));

        // Show timing
        System.out.println("\n" + "-".repeat(60));
        System.out.println("TIMING COMPARISON");
        System.out.println("-".repeat(60));
        System.out.printf("Original time:   %d ms%n", originalResult.getExecutionTimeMs());
        System.out.printf("Optimized time:  %d ms%n", optimizedResult.getExecutionTimeMs());

        long diff = originalResult.getExecutionTimeMs() - optimizedResult.getExecutionTimeMs();
        if (originalResult.getExecutionTimeMs() > 0) {
            double improvement = (diff * 100.0) / originalResult.getExecutionTimeMs();
            System.out.printf("Improvement:     %.1f%%%n", improvement);
        }

        // Validate results are equivalent
        ResultValidator.ValidationResult validation = ResultValidator.validate(originalResult, optimizedResult);
        System.out.println(validation.formatReport());
    }

    private void executeOptimized(DuckDBAdapter duckDB, String sql) throws SQLException {
        System.out.println("\n" + "-".repeat(60));
        System.out.println("QUERY EXECUTION");
        System.out.println("-".repeat(60));

        DuckDBAdapter.QueryResult result = duckDB.executeQuery(sql);

        System.out.println("\nResults:");
        System.out.println(result.formatResults(20));
        System.out.printf("\nExecution time: %d ms%n", result.getExecutionTimeMs());
        System.out.printf("Total rows: %d%n", result.getRowCount());
    }
}
