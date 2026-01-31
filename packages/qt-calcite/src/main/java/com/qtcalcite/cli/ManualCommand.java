package com.qtcalcite.cli;

import com.qtcalcite.calcite.CalciteOptimizer;
import com.qtcalcite.calcite.RuleRegistry;
import com.qtcalcite.config.AppConfig;
import com.qtcalcite.duckdb.DuckDBAdapter;
import com.qtcalcite.duckdb.ResultValidator;
import com.qtcalcite.llm.PromptBuilder;
import com.qtcalcite.llm.ResponseParser;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "manual",
        description = "Manual mode: outputs LLM prompt, user pastes response"
)
public class ManualCommand implements Callable<Integer> {

    @ParentCommand
    private MainCommand parent;

    @Parameters(index = "0", description = "SQL query or @filename for file input")
    private String queryInput;

    @Option(names = {"--dry-run"}, description = "Show optimized SQL without executing")
    private boolean dryRun;

    @Option(names = {"--compare"}, description = "Run both original and optimized, show timing")
    private boolean compare;

    @Option(names = {"--prompt-only"}, description = "Only output the prompt, don't wait for response")
    private boolean promptOnly;

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

            // Connect to DuckDB
            try (DuckDBAdapter duckDB = new DuckDBAdapter(config.getDatabasePath())) {
                // Initialize components
                RuleRegistry ruleRegistry = new RuleRegistry();
                PromptBuilder promptBuilder = new PromptBuilder(ruleRegistry, duckDB);

                // Build and output the prompt
                String prompt = promptBuilder.buildCompletePrompt(sql);
                System.out.println(prompt);

                if (promptOnly) {
                    return 0;
                }

                // Wait for user to paste LLM response
                System.out.println("\nPaste LLM response (rules to apply), then press Enter twice:");
                String response = readMultiLineInput();

                if (response.trim().isEmpty()) {
                    System.err.println("No response provided. Exiting.");
                    return 1;
                }

                // Parse rules from response
                ResponseParser parser = new ResponseParser(ruleRegistry);
                ResponseParser.ParseResult parseResult = parser.parse(response);

                if (!parseResult.hasRules()) {
                    System.err.println("Error: " + parseResult.getError());
                    System.err.println("\nAvailable rules:");
                    for (String ruleName : ruleRegistry.getAvailableRuleNames()) {
                        System.err.println("  - " + ruleName);
                    }
                    return 1;
                }

                List<String> rules = parseResult.getRules();
                System.out.println("\nParsed rules: " + String.join(", ", rules));

                // Apply optimization
                CalciteOptimizer optimizer = new CalciteOptimizer(duckDB);
                CalciteOptimizer.OptimizationResult result = optimizer.optimize(sql, rules);

                System.out.println("\n" + "=".repeat(60));
                System.out.println("OPTIMIZATION RESULT");
                System.out.println("=".repeat(60));
                System.out.println(result.formatSummary());

                if (dryRun) {
                    System.out.println("(Dry run - not executing query)");
                    return 0;
                }

                // Execute queries
                if (compare) {
                    executeComparison(duckDB, sql, result.getOptimizedSql());
                } else {
                    executeOptimized(duckDB, result.getOptimizedSql());
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

    private String readMultiLineInput() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        StringBuilder sb = new StringBuilder();
        String line;
        int emptyLineCount = 0;

        while ((line = reader.readLine()) != null) {
            if (line.isEmpty()) {
                emptyLineCount++;
                if (emptyLineCount >= 1) {
                    break;
                }
            } else {
                emptyLineCount = 0;
                if (sb.length() > 0) sb.append("\n");
                sb.append(line);
            }
        }

        return sb.toString();
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
