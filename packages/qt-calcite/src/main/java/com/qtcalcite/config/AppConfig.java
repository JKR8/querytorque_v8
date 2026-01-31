package com.qtcalcite.config;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class AppConfig {
    private String databasePath;
    private String llmEndpoint;
    private String llmModel;
    private String llmApiKey;
    private double llmTemperature;
    private int llmMaxTokens;
    private boolean verbose;
    private boolean colorOutput;

    private static final String DEFAULT_CONFIG_PATH = "config/application.yaml";

    public AppConfig() {
        // Set defaults
        this.databasePath = "database.duckdb";
        this.llmEndpoint = "https://api.deepseek.com/v1/chat/completions";
        this.llmModel = "deepseek-chat";
        this.llmApiKey = System.getenv("DEEPSEEK_API_KEY");
        this.llmTemperature = 0.0;
        this.llmMaxTokens = 1024;
        this.verbose = false;
        this.colorOutput = true;
    }

    public static AppConfig load(String configPath) {
        AppConfig config = new AppConfig();

        Path path = configPath != null ? Path.of(configPath) : Path.of(DEFAULT_CONFIG_PATH);

        if (!Files.exists(path)) {
            // Try to find config relative to jar location
            String jarDir = AppConfig.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            path = Path.of(jarDir).getParent().resolve("config/application.yaml");
        }

        if (Files.exists(path)) {
            try (InputStream is = new FileInputStream(path.toFile())) {
                Yaml yaml = new Yaml();
                Map<String, Object> data = yaml.load(is);
                config.parseConfig(data);
            } catch (IOException e) {
                System.err.println("Warning: Could not load config file: " + e.getMessage());
            }
        }

        return config;
    }

    @SuppressWarnings("unchecked")
    private void parseConfig(Map<String, Object> data) {
        if (data == null) return;

        // Database config
        Map<String, Object> database = (Map<String, Object>) data.get("database");
        if (database != null) {
            if (database.get("path") != null) {
                this.databasePath = (String) database.get("path");
            }
        }

        // LLM config
        Map<String, Object> llm = (Map<String, Object>) data.get("llm");
        if (llm != null) {
            if (llm.get("endpoint") != null) {
                this.llmEndpoint = (String) llm.get("endpoint");
            }
            if (llm.get("model") != null) {
                this.llmModel = (String) llm.get("model");
            }
            if (llm.get("api_key") != null) {
                String apiKey = (String) llm.get("api_key");
                // Support environment variable substitution
                if (apiKey.startsWith("${") && apiKey.endsWith("}")) {
                    String envVar = apiKey.substring(2, apiKey.length() - 1);
                    this.llmApiKey = System.getenv(envVar);
                } else {
                    this.llmApiKey = apiKey;
                }
            }
            if (llm.get("temperature") != null) {
                this.llmTemperature = ((Number) llm.get("temperature")).doubleValue();
            }
            if (llm.get("max_tokens") != null) {
                this.llmMaxTokens = ((Number) llm.get("max_tokens")).intValue();
            }
        }

        // Output config
        Map<String, Object> output = (Map<String, Object>) data.get("output");
        if (output != null) {
            if (output.get("verbose") != null) {
                this.verbose = (Boolean) output.get("verbose");
            }
            if (output.get("color") != null) {
                this.colorOutput = (Boolean) output.get("color");
            }
        }
    }

    // Getters
    public String getDatabasePath() { return databasePath; }
    public String getLlmEndpoint() { return llmEndpoint; }
    public String getLlmModel() { return llmModel; }
    public String getLlmApiKey() { return llmApiKey; }
    public double getLlmTemperature() { return llmTemperature; }
    public int getLlmMaxTokens() { return llmMaxTokens; }
    public boolean isVerbose() { return verbose; }
    public boolean isColorOutput() { return colorOutput; }

    // Setters for CLI overrides
    public void setDatabasePath(String path) { this.databasePath = path; }
    public void setVerbose(boolean verbose) { this.verbose = verbose; }
}
