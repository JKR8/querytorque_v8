package com.qtcalcite.duckdb;

import java.sql.*;
import java.util.*;

public class DuckDBAdapter implements AutoCloseable {
    private final Connection connection;
    private final String databasePath;

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("DuckDB driver not found", e);
        }
    }

    public DuckDBAdapter(String databasePath) throws SQLException {
        this.databasePath = databasePath;
        this.connection = DriverManager.getConnection("jdbc:duckdb:" + databasePath);
    }

    public Connection getConnection() {
        return connection;
    }

    public String getDatabasePath() {
        return databasePath;
    }

    /**
     * Get the EXPLAIN plan for a SQL query.
     * DuckDB returns: column 1 = explain_key, column 2 = explain_value (the actual plan)
     */
    public String getExplainPlan(String sql) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("EXPLAIN " + sql)) {
            while (rs.next()) {
                // Column 2 contains the actual plan visualization
                String value = rs.getString(2);
                if (value != null) {
                    plan.append(value).append("\n");
                }
            }
        }
        return plan.toString().trim();
    }

    /**
     * Get the EXPLAIN ANALYZE plan for a SQL query (includes actual timings).
     * DuckDB returns: column 1 = explain_key, column 2 = explain_value (the actual plan)
     */
    public String getExplainAnalyze(String sql) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("EXPLAIN ANALYZE " + sql)) {
            while (rs.next()) {
                // Column 2 contains the actual plan visualization
                String value = rs.getString(2);
                if (value != null) {
                    plan.append(value).append("\n");
                }
            }
        }
        return plan.toString().trim();
    }

    /**
     * Execute a query and return results.
     */
    public QueryResult executeQuery(String sql) throws SQLException {
        long startTime = System.currentTimeMillis();
        List<String> columnNames = new ArrayList<>();
        List<List<Object>> rows = new ArrayList<>();

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(meta.getColumnName(i));
            }

            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getObject(i));
                }
                rows.add(row);
            }
        }

        long endTime = System.currentTimeMillis();
        return new QueryResult(columnNames, rows, endTime - startTime);
    }

    /**
     * Get schema information for all tables in the database.
     */
    public Map<String, TableSchema> getSchemaInfo() throws SQLException {
        Map<String, TableSchema> schemas = new LinkedHashMap<>();

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT table_name, column_name, data_type, is_nullable " +
                             "FROM information_schema.columns " +
                             "WHERE table_schema = 'main' " +
                             "ORDER BY table_name, ordinal_position")) {

            String currentTable = null;
            List<ColumnInfo> columns = new ArrayList<>();

            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                boolean nullable = "YES".equals(rs.getString("is_nullable"));

                if (currentTable != null && !currentTable.equals(tableName)) {
                    schemas.put(currentTable, new TableSchema(currentTable, new ArrayList<>(columns)));
                    columns.clear();
                }
                currentTable = tableName;
                columns.add(new ColumnInfo(columnName, dataType, nullable));
            }

            if (currentTable != null && !columns.isEmpty()) {
                schemas.put(currentTable, new TableSchema(currentTable, columns));
            }
        }

        return schemas;
    }

    /**
     * Get list of table names.
     */
    public List<String> getTableNames() throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT table_name FROM information_schema.tables " +
                             "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'")) {
            while (rs.next()) {
                tables.add(rs.getString("table_name"));
            }
        }
        return tables;
    }

    @Override
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // Inner classes for schema representation
    public static class TableSchema {
        private final String tableName;
        private final List<ColumnInfo> columns;

        public TableSchema(String tableName, List<ColumnInfo> columns) {
            this.tableName = tableName;
            this.columns = columns;
        }

        public String getTableName() { return tableName; }
        public List<ColumnInfo> getColumns() { return columns; }
    }

    public static class ColumnInfo {
        private final String name;
        private final String dataType;
        private final boolean nullable;

        public ColumnInfo(String name, String dataType, boolean nullable) {
            this.name = name;
            this.dataType = dataType;
            this.nullable = nullable;
        }

        public String getName() { return name; }
        public String getDataType() { return dataType; }
        public boolean isNullable() { return nullable; }
    }

    public static class QueryResult {
        private final List<String> columnNames;
        private final List<List<Object>> rows;
        private final long executionTimeMs;

        public QueryResult(List<String> columnNames, List<List<Object>> rows, long executionTimeMs) {
            this.columnNames = columnNames;
            this.rows = rows;
            this.executionTimeMs = executionTimeMs;
        }

        public List<String> getColumnNames() { return columnNames; }
        public List<List<Object>> getRows() { return rows; }
        public long getExecutionTimeMs() { return executionTimeMs; }
        public int getRowCount() { return rows.size(); }

        public String formatResults(int maxRows) {
            StringBuilder sb = new StringBuilder();

            if (columnNames.isEmpty()) {
                return "No results";
            }

            // Calculate column widths
            int[] widths = new int[columnNames.size()];
            for (int i = 0; i < columnNames.size(); i++) {
                widths[i] = columnNames.get(i).length();
            }
            for (List<Object> row : rows) {
                for (int i = 0; i < row.size(); i++) {
                    String value = row.get(i) == null ? "NULL" : row.get(i).toString();
                    widths[i] = Math.max(widths[i], Math.min(value.length(), 50));
                }
            }

            // Header
            for (int i = 0; i < columnNames.size(); i++) {
                sb.append(String.format("%-" + widths[i] + "s", columnNames.get(i)));
                if (i < columnNames.size() - 1) sb.append(" | ");
            }
            sb.append("\n");

            // Separator
            for (int i = 0; i < columnNames.size(); i++) {
                sb.append("-".repeat(widths[i]));
                if (i < columnNames.size() - 1) sb.append("-+-");
            }
            sb.append("\n");

            // Rows
            int displayRows = Math.min(rows.size(), maxRows);
            for (int r = 0; r < displayRows; r++) {
                List<Object> row = rows.get(r);
                for (int i = 0; i < row.size(); i++) {
                    String value = row.get(i) == null ? "NULL" : row.get(i).toString();
                    if (value.length() > 50) value = value.substring(0, 47) + "...";
                    sb.append(String.format("%-" + widths[i] + "s", value));
                    if (i < row.size() - 1) sb.append(" | ");
                }
                sb.append("\n");
            }

            if (rows.size() > maxRows) {
                sb.append("... ").append(rows.size() - maxRows).append(" more rows\n");
            }

            return sb.toString();
        }
    }
}
