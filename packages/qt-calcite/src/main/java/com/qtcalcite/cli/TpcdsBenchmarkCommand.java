package com.qtcalcite.cli;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.qtcalcite.calcite.CalciteOptimizer;
import com.qtcalcite.calcite.RuleRegistry;
import com.qtcalcite.config.AppConfig;
import com.qtcalcite.duckdb.DuckDBAdapter;
import com.qtcalcite.duckdb.ResultValidator;
import com.qtcalcite.llm.DeepSeekClient;
import com.qtcalcite.llm.PromptBuilder;
import com.qtcalcite.llm.ResponseParser;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Command(
        name = "tpcds-benchmark",
        description = "Run HEP LLM benchmark on TPC-DS queries with 1-1-2-2 validation"
)
public class TpcdsBenchmarkCommand implements Callable<Integer> {

    @ParentCommand
    private MainCommand parent;

    @Option(names = {"--queries-dir"}, description = "Directory containing TPC-DS query_XX.sql files")
    private String queriesDir = "packages/qt-sql/tests/fixtures/tpcds";

    @Option(names = {"--sample-db"}, description = "Sample DuckDB path (1%%)")
    private String sampleDb = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb";

    @Option(names = {"--full-db"}, description = "Full TPC-DS DuckDB path")
    private String fullDb = System.getenv("TPCDS_PATH") != null
            ? System.getenv("TPCDS_PATH")
            : "/mnt/d/TPC-DS/tpcds_sf100.duckdb";

    @Option(names = {"--skip-sample"}, description = "Skip sample DB run")
    private boolean skipSample;

    @Option(names = {"--skip-full"}, description = "Skip full DB run")
    private boolean skipFull;

    @Option(names = {"--max-queries"}, description = "Maximum number of queries to run (default: 99)")
    private int maxQueries = 99;

    @Option(names = {"--llm-parallelism"}, description = "Number of parallel LLM calls (default: 1)")
    private int llmParallelism = 1;

    @Option(names = {"--output-dir"}, description = "Output directory for results")
    private String outputDir;

    private static final int WARMUP_ORIGINAL = 1;
    private static final int WARMUP_OPTIMIZED = 1;
    private static final int TIMED_ORIGINAL = 2;
    private static final int TIMED_OPTIMIZED = 2;

    @Override
    public Integer call() {
        try {
            AppConfig config = AppConfig.load(parent.getConfigPath());
            if (parent.isVerbose()) {
                config.setVerbose(true);
            }

            DeepSeekClient client = new DeepSeekClient(config);
            if (!client.isConfigured()) {
                System.err.println("Error: DeepSeek API key not configured.");
                return 1;
            }

            List<QueryFile> queries = loadQueries(Paths.get(queriesDir), maxQueries);
            if (queries.isEmpty()) {
                System.err.println("No TPC-DS query files found in: " + queriesDir);
                return 1;
            }

            Path baseOutput = resolveOutputDir();
            Files.createDirectories(baseOutput);

            if (!skipSample) {
                runForDatabase("sample", sampleDb, queries, client, baseOutput);
            }

            if (!skipFull) {
                runForDatabase("full", fullDb, queries, client, baseOutput);
            }

            return 0;
        } catch (Exception e) {
            System.err.println("Benchmark failed: " + e.getMessage());
            e.printStackTrace();
            return 1;
        }
    }

    private void runForDatabase(
            String label,
            String dbPath,
            List<QueryFile> queries,
            DeepSeekClient client,
            Path baseOutput
    ) throws IOException {
        Path dbFile = Paths.get(dbPath);
        if (!Files.exists(dbFile)) {
            System.err.println("Skipping " + label + " DB (not found): " + dbPath);
            return;
        }

        Path outputDir = baseOutput.resolve(label);
        Files.createDirectories(outputDir);

        List<BenchmarkSummary> summaries = new ArrayList<>();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        System.out.println("\n" + "=".repeat(70));
        System.out.println("HEP LLM TPC-DS BENCHMARK (" + label.toUpperCase() + ")");
        System.out.println("Database: " + dbPath);
        System.out.println("Queries: " + queries.size());
        System.out.println("Validation: 1 warmup orig, 1 warmup opt, 2 timed orig, 2 timed opt");
        System.out.println("=".repeat(70));

        try (DuckDBAdapter duckDB = new DuckDBAdapter(dbPath)) {
            RuleRegistry ruleRegistry = new RuleRegistry();
            PromptBuilder promptBuilder = new PromptBuilder(ruleRegistry, duckDB);
            ResponseParser parser = new ResponseParser(ruleRegistry);
            CalciteOptimizer optimizer = new CalciteOptimizer(duckDB);

            List<LLMTask> llmTasks = prepareLLMTasks(queries, promptBuilder, client);

            for (LLMTask task : llmTasks) {
                QueryFile query = task.query;
                String queryName = query.name;
                Path queryDir = outputDir.resolve(queryName);
                Files.createDirectories(queryDir);

                Files.writeString(queryDir.resolve("original.sql"), query.sql);

                BenchmarkSummary summary = new BenchmarkSummary(queryName);
                try {
                    String response = task.response.get();
                    Files.writeString(queryDir.resolve("llm_response.txt"), response);

                    ResponseParser.ParseResult parseResult = parser.parse(response);
                    if (!parseResult.hasRules()) {
                        summary.status = "llm_no_rules";
                        summary.error = parseResult.getError();
                        summaries.add(summary);
                        Files.writeString(queryDir.resolve("error.txt"), summary.error);
                        System.out.println("[" + queryName + "] ERROR - no rules: " + summary.error);
                        continue;
                    }

                    summary.rules = parseResult.getRules();
                    Files.writeString(queryDir.resolve("rules.txt"), String.join(", ", summary.rules));

                    CalciteOptimizer.OptimizationResult optResult = optimizer.optimize(query.sql, summary.rules);
                    summary.optimizedSql = optResult.getOptimizedSql();
                    Files.writeString(queryDir.resolve("optimized.sql"), summary.optimizedSql);

                    PlanMetrics planMetrics = buildPlanMetrics(duckDB, query.sql, summary.optimizedSql);
                    summary.originalPlanCost = planMetrics.originalPlanCost;
                    summary.optimizedPlanCost = planMetrics.optimizedPlanCost;
                    summary.originalPlanMaxRows = planMetrics.originalPlanMaxRows;
                    summary.optimizedPlanMaxRows = planMetrics.optimizedPlanMaxRows;

                    ValidationRun run = runValidation1122(duckDB, query.sql, summary.optimizedSql);
                    summary.originalTimesMs = run.originalTimesMs;
                    summary.optimizedTimesMs = run.optimizedTimesMs;
                    summary.originalAvgMs = run.originalAvgMs;
                    summary.optimizedAvgMs = run.optimizedAvgMs;
                    summary.speedup = run.speedup;
                    summary.originalRowCount = run.originalResult.getRowCount();
                    summary.optimizedRowCount = run.optimizedResult.getRowCount();

                    ResultValidator.ValidationResult validation =
                            ResultValidator.validate(run.originalResult, run.optimizedResult);
                    summary.validationPassed = validation.isValid() || validation.isUnorderedChecksumMatch();

                    Files.writeString(queryDir.resolve("validation.txt"), validation.formatReport());
                    Files.writeString(queryDir.resolve("timing.json"), gson.toJson(summary));

                    summary.status = "success";
                    summaries.add(summary);

                    System.out.println("[" + queryName + "] " +
                            String.format("%.2fx", summary.speedup) +
                            " (orig " + summary.originalAvgMs + "ms, opt " + summary.optimizedAvgMs + "ms)");
                } catch (Exception e) {
                    summary.status = "error";
                    summary.error = e.getMessage();
                    summaries.add(summary);
                    Files.writeString(queryDir.resolve("error.txt"), summary.error == null ? "Unknown error" : summary.error);
                    System.out.println("[" + queryName + "] ERROR - " + summary.error);
                }
            }
        } catch (SQLException e) {
            System.err.println("Database error for " + label + " DB: " + e.getMessage());
            return;
        }

        Files.writeString(outputDir.resolve("results.json"), gson.toJson(summaries));
        Files.writeString(outputDir.resolve("summary.txt"), buildSummaryText(label, dbPath, summaries));
    }

    private ValidationRun runValidation1122(DuckDBAdapter duckDB, String originalSql, String optimizedSql)
            throws SQLException {
        for (int i = 0; i < WARMUP_ORIGINAL; i++) {
            duckDB.executeQuery(originalSql);
        }
        for (int i = 0; i < WARMUP_OPTIMIZED; i++) {
            duckDB.executeQuery(optimizedSql);
        }

        List<Long> originalTimes = new ArrayList<>();
        List<Long> optimizedTimes = new ArrayList<>();

        DuckDBAdapter.QueryResult originalResult = null;
        DuckDBAdapter.QueryResult optimizedResult = null;

        for (int i = 0; i < TIMED_ORIGINAL; i++) {
            DuckDBAdapter.QueryResult result = duckDB.executeQuery(originalSql);
            originalTimes.add(result.getExecutionTimeMs());
            if (i == 0) {
                originalResult = result;
            }
        }

        for (int i = 0; i < TIMED_OPTIMIZED; i++) {
            DuckDBAdapter.QueryResult result = duckDB.executeQuery(optimizedSql);
            optimizedTimes.add(result.getExecutionTimeMs());
            if (i == 0) {
                optimizedResult = result;
            }
        }

        long originalAvg = average(originalTimes);
        long optimizedAvg = average(optimizedTimes);
        double speedup = optimizedAvg > 0 ? (double) originalAvg / optimizedAvg : 1.0;

        return new ValidationRun(
                originalTimes,
                optimizedTimes,
                originalAvg,
                optimizedAvg,
                speedup,
                originalResult,
                optimizedResult
        );
    }

    private long average(List<Long> values) {
        if (values.isEmpty()) {
            return 0;
        }
        long sum = 0;
        for (Long value : values) {
            sum += value;
        }
        return sum / values.size();
    }

    private PlanMetrics buildPlanMetrics(DuckDBAdapter duckDB, String originalSql, String optimizedSql)
            throws SQLException {
        String originalPlan = duckDB.getExplainPlan(originalSql);
        String optimizedPlan = duckDB.getExplainPlan(optimizedSql);

        return new PlanMetrics(
                extractPlanCost(originalPlan),
                extractPlanCost(optimizedPlan),
                extractMaxRows(originalPlan),
                extractMaxRows(optimizedPlan)
        );
    }

    private Double extractPlanCost(String plan) {
        if (plan == null || plan.isEmpty()) {
            return null;
        }
        List<Double> candidates = new ArrayList<>();

        Pattern costPattern = Pattern.compile("cost\\s*[=:]\\s*([0-9.eE+-]+)(?:\\s*\\.\\.\\s*([0-9.eE+-]+))?");
        Matcher costMatcher = costPattern.matcher(plan);
        while (costMatcher.find()) {
            String end = costMatcher.group(2) != null ? costMatcher.group(2) : costMatcher.group(1);
            try {
                candidates.add(Double.parseDouble(end));
            } catch (NumberFormatException ignored) {
            }
        }

        Pattern totalCost = Pattern.compile("total\\s+cost\\s*[=:]\\s*([0-9.eE+-]+)", Pattern.CASE_INSENSITIVE);
        Matcher totalMatcher = totalCost.matcher(plan);
        while (totalMatcher.find()) {
            try {
                candidates.add(Double.parseDouble(totalMatcher.group(1)));
            } catch (NumberFormatException ignored) {
            }
        }

        return candidates.stream().max(Double::compareTo).orElse(null);
    }

    private Long extractMaxRows(String plan) {
        if (plan == null || plan.isEmpty()) {
            return null;
        }
        List<Long> candidates = new ArrayList<>();

        Pattern rowsPattern = Pattern.compile("rows\\s*[=:]\\s*([0-9]+)", Pattern.CASE_INSENSITIVE);
        Matcher rowsMatcher = rowsPattern.matcher(plan);
        while (rowsMatcher.find()) {
            try {
                candidates.add(Long.parseLong(rowsMatcher.group(1)));
            } catch (NumberFormatException ignored) {
            }
        }

        Pattern altRowsPattern = Pattern.compile("~?\\s*([0-9]+)\\s*rows", Pattern.CASE_INSENSITIVE);
        Matcher altRowsMatcher = altRowsPattern.matcher(plan);
        while (altRowsMatcher.find()) {
            try {
                candidates.add(Long.parseLong(altRowsMatcher.group(1)));
            } catch (NumberFormatException ignored) {
            }
        }

        return candidates.stream().max(Long::compareTo).orElse(null);
    }

    private Path resolveOutputDir() {
        if (outputDir != null && !outputDir.isBlank()) {
            return Paths.get(outputDir);
        }
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return Paths.get("research/experiments/hep_llm_bench", timestamp);
    }

    private List<QueryFile> loadQueries(Path dir, int limit) throws IOException {
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            return List.of();
        }

        Pattern pattern = Pattern.compile("query_(\\d+)\\.sql");
        List<QueryFile> queries = Files.list(dir)
                .filter(path -> pattern.matcher(path.getFileName().toString()).matches())
                .map(path -> {
                    Matcher matcher = pattern.matcher(path.getFileName().toString());
                    if (!matcher.matches()) {
                        return null;
                    }
                    int id = Integer.parseInt(matcher.group(1));
                    try {
                        String sql = sanitizeSql(Files.readString(path));
                        return new QueryFile("q" + id, id, sql);
                    } catch (IOException e) {
                        return null;
                    }
                })
                .filter(q -> q != null && !q.sql.isEmpty())
                .sorted(Comparator.comparingInt(q -> q.id))
                .limit(limit)
                .collect(Collectors.toList());

        return queries;
    }

    private String sanitizeSql(String sql) {
        if (sql == null) {
            return "";
        }
        String trimmed = sql.trim();
        while (trimmed.endsWith(";")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
        }
        return trimmed;
    }

    private List<LLMTask> prepareLLMTasks(
            List<QueryFile> queries,
            PromptBuilder promptBuilder,
            DeepSeekClient client
    ) {
        int parallelism = Math.max(1, llmParallelism);
        java.util.concurrent.ExecutorService executor =
                java.util.concurrent.Executors.newFixedThreadPool(parallelism);
        List<LLMTask> tasks = new ArrayList<>();

        for (QueryFile query : queries) {
            java.util.concurrent.CompletableFuture<String> future =
                    java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                        try {
                            PromptBuilder.LLMPrompt prompt = promptBuilder.buildLLMPrompt(query.sql);
                            return client.chat(prompt);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }, executor);
            tasks.add(new LLMTask(query, future));
        }

        executor.shutdown();
        return tasks;
    }

    private String buildSummaryText(String label, String dbPath, List<BenchmarkSummary> summaries) {
        long success = summaries.stream().filter(s -> "success".equals(s.status)).count();
        long errors = summaries.size() - success;
        Optional<BenchmarkSummary> best = summaries.stream()
                .filter(s -> s.status.equals("success"))
                .max(Comparator.comparingDouble(s -> s.speedup));

        StringBuilder sb = new StringBuilder();
        sb.append("HEP LLM TPC-DS Benchmark (").append(label).append(")\n");
        sb.append("Database: ").append(dbPath).append("\n");
        sb.append("Queries: ").append(summaries.size()).append("\n");
        sb.append("Success: ").append(success).append("\n");
        sb.append("Errors: ").append(errors).append("\n");
        if (best.isPresent()) {
            BenchmarkSummary b = best.get();
            sb.append("Best speedup: ").append(b.query).append(" ")
                    .append(String.format("%.2fx", b.speedup)).append("\n");
        }
        return sb.toString();
    }

    private static class QueryFile {
        private final String name;
        private final int id;
        private final String sql;

        private QueryFile(String name, int id, String sql) {
            this.name = name;
            this.id = id;
            this.sql = sql;
        }
    }

    private static class PlanMetrics {
        private final Double originalPlanCost;
        private final Double optimizedPlanCost;
        private final Long originalPlanMaxRows;
        private final Long optimizedPlanMaxRows;

        private PlanMetrics(Double originalPlanCost, Double optimizedPlanCost,
                            Long originalPlanMaxRows, Long optimizedPlanMaxRows) {
            this.originalPlanCost = originalPlanCost;
            this.optimizedPlanCost = optimizedPlanCost;
            this.originalPlanMaxRows = originalPlanMaxRows;
            this.optimizedPlanMaxRows = optimizedPlanMaxRows;
        }
    }

    private static class ValidationRun {
        private final List<Long> originalTimesMs;
        private final List<Long> optimizedTimesMs;
        private final long originalAvgMs;
        private final long optimizedAvgMs;
        private final double speedup;
        private final DuckDBAdapter.QueryResult originalResult;
        private final DuckDBAdapter.QueryResult optimizedResult;

        private ValidationRun(List<Long> originalTimesMs,
                              List<Long> optimizedTimesMs,
                              long originalAvgMs,
                              long optimizedAvgMs,
                              double speedup,
                              DuckDBAdapter.QueryResult originalResult,
                              DuckDBAdapter.QueryResult optimizedResult) {
            this.originalTimesMs = originalTimesMs;
            this.optimizedTimesMs = optimizedTimesMs;
            this.originalAvgMs = originalAvgMs;
            this.optimizedAvgMs = optimizedAvgMs;
            this.speedup = speedup;
            this.originalResult = originalResult;
            this.optimizedResult = optimizedResult;
        }
    }

    private static class BenchmarkSummary {
        private final String query;
        private String status = "pending";
        private String error;
        private List<String> rules = List.of();
        private String optimizedSql;
        private List<Long> originalTimesMs = List.of();
        private List<Long> optimizedTimesMs = List.of();
        private long originalAvgMs;
        private long optimizedAvgMs;
        private double speedup;
        private int originalRowCount;
        private int optimizedRowCount;
        private boolean validationPassed;
        private Double originalPlanCost;
        private Double optimizedPlanCost;
        private Long originalPlanMaxRows;
        private Long optimizedPlanMaxRows;

        private BenchmarkSummary(String query) {
            this.query = query;
        }
    }

    private static class LLMTask {
        private final QueryFile query;
        private final java.util.concurrent.CompletableFuture<String> response;

        private LLMTask(QueryFile query, java.util.concurrent.CompletableFuture<String> response) {
            this.query = query;
            this.response = response;
        }
    }
}
