package com.qtcalcite.calcite;

import com.qtcalcite.duckdb.DuckDBAdapter;

import java.sql.*;
import java.util.*;

/**
 * Fetches real statistics from DuckDB for cost-based optimization.
 */
public class DuckDBStatistics {

    private final Map<String, TableStats> tableStats = new HashMap<>();
    private final Map<String, Map<String, ColumnStats>> columnStats = new HashMap<>();

    public DuckDBStatistics(DuckDBAdapter adapter) throws SQLException {
        loadStatistics(adapter.getConnection());
    }

    private void loadStatistics(Connection conn) throws SQLException {
        // Load table row counts
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT table_name, estimated_size, column_count " +
                     "FROM duckdb_tables() WHERE schema_name='main'")) {
            while (rs.next()) {
                String tableName = rs.getString(1).toLowerCase();
                long rowCount = rs.getLong(2);
                int colCount = rs.getInt(3);
                tableStats.put(tableName, new TableStats(tableName, rowCount, colCount));
            }
        }

        // Load column stats for each table
        for (String tableName : tableStats.keySet()) {
            Map<String, ColumnStats> cols = new HashMap<>();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT column_name, data_type FROM duckdb_columns() " +
                         "WHERE table_name='" + tableName + "'")) {
                while (rs.next()) {
                    String colName = rs.getString(1).toLowerCase();
                    String dataType = rs.getString(2);
                    cols.put(colName, new ColumnStats(colName, dataType));
                }
            }

            // Get detailed stats from storage info
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "CALL pragma_storage_info('" + tableName + "')")) {
                while (rs.next()) {
                    String colName = rs.getString("column_name").toLowerCase();
                    String statsStr = rs.getString("stats");
                    ColumnStats cs = cols.get(colName);
                    if (cs != null && statsStr != null) {
                        cs.parseStats(statsStr);
                    }
                }
            } catch (SQLException e) {
                // pragma_storage_info might not work for all tables
            }

            columnStats.put(tableName, cols);
        }
    }

    public long getRowCount(String tableName) {
        TableStats ts = tableStats.get(tableName.toLowerCase());
        return ts != null ? ts.rowCount : 1000; // default estimate
    }

    public double getSelectivity(String tableName, String columnName) {
        Map<String, ColumnStats> cols = columnStats.get(tableName.toLowerCase());
        if (cols == null) return 0.1; // default 10% selectivity

        ColumnStats cs = cols.get(columnName.toLowerCase());
        if (cs == null) return 0.1;

        // Estimate selectivity based on distinct values
        long tableRows = getRowCount(tableName);
        if (cs.distinctCount > 0 && tableRows > 0) {
            return 1.0 / cs.distinctCount;
        }
        return 0.1;
    }

    public Set<String> getTableNames() {
        return tableStats.keySet();
    }

    public TableStats getTableStats(String tableName) {
        return tableStats.get(tableName.toLowerCase());
    }

    public ColumnStats getColumnStats(String tableName, String columnName) {
        Map<String, ColumnStats> cols = columnStats.get(tableName.toLowerCase());
        return cols != null ? cols.get(columnName.toLowerCase()) : null;
    }

    public long getDistinctCount(String tableName, String columnName) {
        ColumnStats cs = getColumnStats(tableName, columnName);
        return cs != null ? cs.distinctCount : 0;
    }

    public static class TableStats {
        public final String tableName;
        public final long rowCount;
        public final int columnCount;

        public TableStats(String tableName, long rowCount, int columnCount) {
            this.tableName = tableName;
            this.rowCount = rowCount;
            this.columnCount = columnCount;
        }
    }

    public static class ColumnStats {
        public final String columnName;
        public final String dataType;
        public String minValue;
        public String maxValue;
        public boolean hasNull = false;
        public long distinctCount = 0;

        public ColumnStats(String columnName, String dataType) {
            this.columnName = columnName;
            this.dataType = dataType;
        }

        void parseStats(String stats) {
            // Parse: [Min: 1, Max: 122880][Has Null: false, Has No Null: true]
            if (stats.contains("Min:")) {
                int start = stats.indexOf("Min:") + 4;
                int end = stats.indexOf(",", start);
                if (end > start) {
                    minValue = stats.substring(start, end).trim();
                }
            }
            if (stats.contains("Max:")) {
                int start = stats.indexOf("Max:") + 4;
                int end = stats.indexOf("]", start);
                if (end > start) {
                    maxValue = stats.substring(start, end).trim();
                }
            }
            hasNull = stats.contains("Has Null: true");

            // Estimate distinct count from min/max for numeric types
            if (minValue != null && maxValue != null) {
                try {
                    long min = Long.parseLong(minValue);
                    long max = Long.parseLong(maxValue);
                    distinctCount = max - min + 1;
                } catch (NumberFormatException e) {
                    // Not numeric, estimate based on type
                    distinctCount = 1000; // default
                }
            }
        }
    }
}
