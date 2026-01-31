package com.qtcalcite.llm;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.qtcalcite.config.AppConfig;
import okhttp3.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Client for DeepSeek API (OpenAI-compatible).
 */
public class DeepSeekClient {

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final Gson gson = new Gson();

    private final OkHttpClient httpClient;
    private final String endpoint;
    private final String model;
    private final String apiKey;
    private final double temperature;
    private final int maxTokens;

    public DeepSeekClient(AppConfig config) {
        this.endpoint = config.getLlmEndpoint();
        this.model = config.getLlmModel();
        this.apiKey = config.getLlmApiKey();
        this.temperature = config.getLlmTemperature();
        this.maxTokens = config.getLlmMaxTokens();

        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(120, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    /**
     * Check if the client is configured with an API key.
     */
    public boolean isConfigured() {
        return apiKey != null && !apiKey.isEmpty();
    }

    /**
     * Send a chat completion request to DeepSeek API.
     */
    public String chat(PromptBuilder.LLMPrompt prompt) throws IOException, ApiException {
        if (!isConfigured()) {
            throw new ApiException("DeepSeek API key not configured. Set DEEPSEEK_API_KEY environment variable.");
        }

        JsonObject requestBody = buildRequestBody(prompt);

        Request request = new Request.Builder()
                .url(endpoint)
                .addHeader("Authorization", "Bearer " + apiKey)
                .addHeader("Content-Type", "application/json")
                .post(RequestBody.create(requestBody.toString(), JSON))
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "";

            if (!response.isSuccessful()) {
                throw new ApiException("API request failed with status " + response.code() + ": " + responseBody);
            }

            return extractContent(responseBody);
        }
    }

    private JsonObject buildRequestBody(PromptBuilder.LLMPrompt prompt) {
        JsonObject body = new JsonObject();
        body.addProperty("model", model);
        body.addProperty("temperature", temperature);
        body.addProperty("max_tokens", maxTokens);

        JsonArray messages = new JsonArray();

        // System message
        JsonObject systemMessage = new JsonObject();
        systemMessage.addProperty("role", "system");
        systemMessage.addProperty("content", prompt.getSystemPrompt());
        messages.add(systemMessage);

        // User message
        JsonObject userMessage = new JsonObject();
        userMessage.addProperty("role", "user");
        userMessage.addProperty("content", prompt.getUserPrompt());
        messages.add(userMessage);

        body.add("messages", messages);

        return body;
    }

    private String extractContent(String responseBody) throws ApiException {
        try {
            JsonObject response = gson.fromJson(responseBody, JsonObject.class);

            if (response.has("error")) {
                JsonObject error = response.getAsJsonObject("error");
                String message = error.has("message") ? error.get("message").getAsString() : "Unknown error";
                throw new ApiException("API error: " + message);
            }

            JsonArray choices = response.getAsJsonArray("choices");
            if (choices == null || choices.isEmpty()) {
                throw new ApiException("No choices in API response");
            }

            JsonObject firstChoice = choices.get(0).getAsJsonObject();
            JsonObject message = firstChoice.getAsJsonObject("message");
            if (message == null) {
                throw new ApiException("No message in API response");
            }

            return message.get("content").getAsString();
        } catch (Exception e) {
            if (e instanceof ApiException) throw (ApiException) e;
            throw new ApiException("Failed to parse API response: " + e.getMessage());
        }
    }

    /**
     * API exception class.
     */
    public static class ApiException extends Exception {
        public ApiException(String message) {
            super(message);
        }
    }
}
