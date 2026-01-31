package com.qtcalcite.calcite;

import com.qtcalcite.duckdb.DuckDBAdapter;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Creates Calcite schema from DuckDB database metadata.
 */
public class DuckDBSchemaFactory {

    private final DuckDBAdapter adapter;

    public DuckDBSchemaFactory(DuckDBAdapter adapter) {
        this.adapter = adapter;
    }

    /**
     * Create a Calcite schema from the DuckDB database.
     */
    public Schema createSchema() throws SQLException {
        Map<String, DuckDBAdapter.TableSchema> schemaInfo = adapter.getSchemaInfo();
        return new DuckDBSchema(schemaInfo);
    }

    /**
     * Add DuckDB schema to a Calcite SchemaPlus.
     */
    public void addToSchema(SchemaPlus parentSchema, String schemaName) throws SQLException {
        Schema duckDbSchema = createSchema();
        parentSchema.add(schemaName, duckDbSchema);
    }

    /**
     * Calcite schema implementation backed by DuckDB metadata.
     */
    private static class DuckDBSchema extends AbstractSchema {
        private final Map<String, Table> tableMap;

        public DuckDBSchema(Map<String, DuckDBAdapter.TableSchema> schemaInfo) {
            this.tableMap = new LinkedHashMap<>();
            for (Map.Entry<String, DuckDBAdapter.TableSchema> entry : schemaInfo.entrySet()) {
                tableMap.put(entry.getKey(), new DuckDBTable(entry.getValue()));
            }
        }

        @Override
        protected Map<String, Table> getTableMap() {
            return tableMap;
        }
    }

    /**
     * Calcite table implementation for a DuckDB table.
     * Implements ScannableTable to enable BINDABLE convention conversion rules.
     * Note: The scan() method returns empty data - we use this for optimization only, not execution.
     */
    private static class DuckDBTable extends AbstractTable implements ScannableTable {
        private final DuckDBAdapter.TableSchema schema;

        public DuckDBTable(DuckDBAdapter.TableSchema schema) {
            this.schema = schema;
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            // Return empty enumerable - we use this for optimization only, not execution
            return Linq4j.asEnumerable(Collections.<Object[]>emptyList());
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = typeFactory.builder();

            for (DuckDBAdapter.ColumnInfo col : schema.getColumns()) {
                SqlTypeName sqlType = mapDuckDBType(col.getDataType());
                RelDataType type = typeFactory.createSqlType(sqlType);
                if (col.isNullable()) {
                    type = typeFactory.createTypeWithNullability(type, true);
                }
                builder.add(col.getName(), type);
            }

            return builder.build();
        }

        /**
         * Map DuckDB data types to Calcite SQL types.
         */
        private SqlTypeName mapDuckDBType(String duckdbType) {
            if (duckdbType == null) return SqlTypeName.ANY;

            String upper = duckdbType.toUpperCase();

            // Integer types
            if (upper.contains("BIGINT") || upper.equals("INT8") || upper.equals("LONG")) {
                return SqlTypeName.BIGINT;
            }
            if (upper.contains("INTEGER") || upper.equals("INT4") || upper.equals("INT") || upper.equals("SIGNED")) {
                return SqlTypeName.INTEGER;
            }
            if (upper.contains("SMALLINT") || upper.equals("INT2") || upper.equals("SHORT")) {
                return SqlTypeName.SMALLINT;
            }
            if (upper.contains("TINYINT") || upper.equals("INT1")) {
                return SqlTypeName.TINYINT;
            }
            if (upper.contains("HUGEINT")) {
                return SqlTypeName.DECIMAL;
            }

            // Floating point types
            if (upper.contains("DOUBLE") || upper.equals("FLOAT8") || upper.equals("NUMERIC")) {
                return SqlTypeName.DOUBLE;
            }
            if (upper.contains("REAL") || upper.equals("FLOAT4") || upper.equals("FLOAT")) {
                return SqlTypeName.REAL;
            }

            // Decimal/Numeric
            if (upper.startsWith("DECIMAL") || upper.startsWith("NUMERIC")) {
                return SqlTypeName.DECIMAL;
            }

            // String types
            if (upper.contains("VARCHAR") || upper.equals("STRING") || upper.equals("TEXT")) {
                return SqlTypeName.VARCHAR;
            }
            if (upper.equals("CHAR") || upper.startsWith("CHAR(")) {
                return SqlTypeName.CHAR;
            }

            // Boolean
            if (upper.equals("BOOLEAN") || upper.equals("BOOL") || upper.equals("LOGICAL")) {
                return SqlTypeName.BOOLEAN;
            }

            // Date/Time types
            if (upper.equals("DATE")) {
                return SqlTypeName.DATE;
            }
            if (upper.equals("TIME") || upper.contains("TIME WITHOUT")) {
                return SqlTypeName.TIME;
            }
            if (upper.contains("TIMESTAMP") || upper.equals("DATETIME")) {
                return SqlTypeName.TIMESTAMP;
            }
            if (upper.contains("INTERVAL")) {
                return SqlTypeName.INTERVAL_DAY;
            }

            // Binary types
            if (upper.equals("BLOB") || upper.equals("BYTEA") || upper.contains("BINARY")) {
                return SqlTypeName.VARBINARY;
            }

            // UUID
            if (upper.equals("UUID")) {
                return SqlTypeName.CHAR; // Store as CHAR(36)
            }

            // JSON
            if (upper.equals("JSON")) {
                return SqlTypeName.VARCHAR;
            }

            // Array types - just use ANY for now
            if (upper.endsWith("[]") || upper.startsWith("LIST")) {
                return SqlTypeName.ARRAY;
            }

            // Map/Struct types
            if (upper.startsWith("MAP") || upper.startsWith("STRUCT")) {
                return SqlTypeName.OTHER;
            }

            // Default fallback
            return SqlTypeName.VARCHAR;
        }
    }
}
