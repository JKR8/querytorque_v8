package com.qtcalcite;

import com.qtcalcite.calcite.VolcanoOptimizer;
import com.qtcalcite.duckdb.DuckDBAdapter;
import org.junit.jupiter.api.*;

import java.util.List;

/**
 * Diagnostic: Compare DuckDB plan vs Calcite optimized SQL
 */
public class DiagnosticTest {

    private static final String TPCDS_PATH = "/mnt/d/TPC-DS/tpcds_sf100.duckdb";
    private static DuckDBAdapter adapter;
    private static VolcanoOptimizer optimizer;

    @BeforeAll
    static void setup() throws Exception {
        adapter = new DuckDBAdapter(TPCDS_PATH);
        optimizer = new VolcanoOptimizer(adapter);
    }

    @AfterAll
    static void teardown() throws Exception {
        if (adapter != null) adapter.close();
    }

    @Test
    void diagnoseQ37() throws Exception {
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

        System.out.println("=== ORIGINAL SQL ===");
        System.out.println(sql);

        System.out.println("\n=== DUCKDB EXPLAIN (what DuckDB chooses) ===");
        System.out.println(adapter.getExplainPlan(sql));

        System.out.println("\n=== CALCITE OPTIMIZATION ===");
        List<String> rules = List.of(
            "JOIN_COMMUTE", "JOIN_ASSOCIATE", "JOIN_TO_MULTI_JOIN",
            "MULTI_JOIN_OPTIMIZE", "FILTER_INTO_JOIN"
        );

        VolcanoOptimizer.OptimizationResult result = optimizer.optimize(sql, rules);

        if (result.hasError()) {
            System.out.println("Error: " + result.getError());
        } else {
            System.out.println("Original cost: " + result.getOriginalCost());
            System.out.println("Optimized cost: " + result.getOptimizedCost());
            System.out.println("\nOptimized SQL:");
            System.out.println(result.getOptimizedSql());

            // Clean for DuckDB
            String cleanSql = result.getOptimizedSql()
                .replaceAll("`?DUCKDB`?\\.", "")
                .replace("`", "\"")
                .replaceAll("FETCH NEXT (\\d+) ROWS ONLY", "LIMIT $1");

            System.out.println("\n=== DUCKDB EXPLAIN (Calcite optimized) ===");
            try {
                System.out.println(adapter.getExplainPlan(cleanSql));
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
                System.out.println("\nCleaned SQL that failed:");
                System.out.println(cleanSql);
            }
        }
    }
}
