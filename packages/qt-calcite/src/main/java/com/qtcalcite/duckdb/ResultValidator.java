package com.qtcalcite.duckdb;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Validates that two query results are equivalent.
 * Checks row count, ordering, and content checksum.
 */
public class ResultValidator {

    /**
     * Validate that two query results are equivalent.
     */
    public static ValidationResult validate(DuckDBAdapter.QueryResult original,
                                            DuckDBAdapter.QueryResult optimized) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        // 1. Check column count
        if (original.getColumnNames().size() != optimized.getColumnNames().size()) {
            errors.add(String.format("Column count mismatch: original=%d, optimized=%d",
                    original.getColumnNames().size(), optimized.getColumnNames().size()));
        }

        // 2. Check row count
        if (original.getRowCount() != optimized.getRowCount()) {
            errors.add(String.format("Row count mismatch: original=%d, optimized=%d",
                    original.getRowCount(), optimized.getRowCount()));
        }

        // 3. Check column names match (may differ in case/aliasing)
        for (int i = 0; i < Math.min(original.getColumnNames().size(), optimized.getColumnNames().size()); i++) {
            String origCol = original.getColumnNames().get(i).toLowerCase();
            String optCol = optimized.getColumnNames().get(i).toLowerCase();
            // Allow NAME vs name differences from Calcite aliasing
            if (!origCol.equals(optCol) && !origCol.replace("_", "").equals(optCol.replace("_", ""))) {
                warnings.add(String.format("Column name differs at position %d: '%s' vs '%s'",
                        i, original.getColumnNames().get(i), optimized.getColumnNames().get(i)));
            }
        }

        // 4. Check row ordering (compare row by row)
        boolean orderMatch = true;
        int mismatchRow = -1;
        int rowsToCheck = Math.min(original.getRowCount(), optimized.getRowCount());

        for (int r = 0; r < rowsToCheck; r++) {
            List<Object> origRow = original.getRows().get(r);
            List<Object> optRow = optimized.getRows().get(r);

            if (!rowsEqual(origRow, optRow)) {
                orderMatch = false;
                mismatchRow = r;
                break;
            }
        }

        if (!orderMatch) {
            warnings.add(String.format("Row ordering differs starting at row %d", mismatchRow));

            // Check if it's just ordering or actual content difference
            if (original.getRowCount() == optimized.getRowCount()) {
                boolean contentMatch = checkContentMatch(original, optimized);
                if (contentMatch) {
                    warnings.add("Content is equivalent but ordering differs (may be acceptable if no ORDER BY)");
                } else {
                    errors.add("Row content differs - results are not equivalent");
                }
            }
        }

        // 5. Compute and compare checksums
        String origChecksum = computeChecksum(original);
        String optChecksum = computeChecksum(optimized);

        boolean checksumMatch = origChecksum.equals(optChecksum);
        if (!checksumMatch && orderMatch) {
            // Checksums differ but order matched - shouldn't happen
            errors.add("Checksum mismatch despite row-by-row match");
        }

        // 6. Compute order-independent checksum
        String origUnorderedChecksum = computeUnorderedChecksum(original);
        String optUnorderedChecksum = computeUnorderedChecksum(optimized);
        boolean unorderedChecksumMatch = origUnorderedChecksum.equals(optUnorderedChecksum);

        return new ValidationResult(
                errors.isEmpty(),
                errors,
                warnings,
                original.getRowCount(),
                optimized.getRowCount(),
                origChecksum,
                optChecksum,
                checksumMatch,
                origUnorderedChecksum,
                optUnorderedChecksum,
                unorderedChecksumMatch,
                orderMatch
        );
    }

    private static boolean rowsEqual(List<Object> row1, List<Object> row2) {
        if (row1.size() != row2.size()) return false;

        for (int i = 0; i < row1.size(); i++) {
            Object v1 = row1.get(i);
            Object v2 = row2.get(i);

            if (!valuesEqual(v1, v2)) {
                return false;
            }
        }
        return true;
    }

    private static boolean valuesEqual(Object v1, Object v2) {
        if (v1 == null && v2 == null) return true;
        if (v1 == null || v2 == null) return false;

        // Handle numeric comparisons with tolerance
        if (v1 instanceof Number && v2 instanceof Number) {
            double d1 = ((Number) v1).doubleValue();
            double d2 = ((Number) v2).doubleValue();
            return Math.abs(d1 - d2) < 0.0001;
        }

        // String comparison
        return Objects.equals(v1.toString(), v2.toString());
    }

    /**
     * Check if content matches regardless of order.
     */
    private static boolean checkContentMatch(DuckDBAdapter.QueryResult original,
                                             DuckDBAdapter.QueryResult optimized) {
        if (original.getRowCount() != optimized.getRowCount()) {
            return false;
        }

        // Convert rows to strings and sort for comparison
        List<String> origStrings = new ArrayList<>();
        List<String> optStrings = new ArrayList<>();

        for (List<Object> row : original.getRows()) {
            origStrings.add(rowToString(row));
        }
        for (List<Object> row : optimized.getRows()) {
            optStrings.add(rowToString(row));
        }

        origStrings.sort(String::compareTo);
        optStrings.sort(String::compareTo);

        return origStrings.equals(optStrings);
    }

    private static String rowToString(List<Object> row) {
        StringBuilder sb = new StringBuilder();
        for (Object val : row) {
            if (val == null) {
                sb.append("NULL");
            } else if (val instanceof Number) {
                // Normalize numeric values
                sb.append(String.format("%.6f", ((Number) val).doubleValue()));
            } else {
                sb.append(val.toString());
            }
            sb.append("|");
        }
        return sb.toString();
    }

    /**
     * Compute ordered checksum (sensitive to row order).
     */
    private static String computeChecksum(DuckDBAdapter.QueryResult result) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");

            for (List<Object> row : result.getRows()) {
                md.update(rowToString(row).getBytes());
            }

            byte[] digest = md.digest();
            return new BigInteger(1, digest).toString(16);
        } catch (NoSuchAlgorithmException e) {
            return "error";
        }
    }

    /**
     * Compute order-independent checksum (XOR of row hashes).
     */
    private static String computeUnorderedChecksum(DuckDBAdapter.QueryResult result) {
        long xorHash = 0;

        for (List<Object> row : result.getRows()) {
            xorHash ^= rowToString(row).hashCode();
        }

        return Long.toHexString(xorHash);
    }

    /**
     * Validation result container.
     */
    public static class ValidationResult {
        private final boolean valid;
        private final List<String> errors;
        private final List<String> warnings;
        private final int originalRowCount;
        private final int optimizedRowCount;
        private final String originalChecksum;
        private final String optimizedChecksum;
        private final boolean checksumMatch;
        private final String originalUnorderedChecksum;
        private final String optimizedUnorderedChecksum;
        private final boolean unorderedChecksumMatch;
        private final boolean orderMatch;

        public ValidationResult(boolean valid, List<String> errors, List<String> warnings,
                                int originalRowCount, int optimizedRowCount,
                                String originalChecksum, String optimizedChecksum, boolean checksumMatch,
                                String originalUnorderedChecksum, String optimizedUnorderedChecksum,
                                boolean unorderedChecksumMatch, boolean orderMatch) {
            this.valid = valid;
            this.errors = errors;
            this.warnings = warnings;
            this.originalRowCount = originalRowCount;
            this.optimizedRowCount = optimizedRowCount;
            this.originalChecksum = originalChecksum;
            this.optimizedChecksum = optimizedChecksum;
            this.checksumMatch = checksumMatch;
            this.originalUnorderedChecksum = originalUnorderedChecksum;
            this.optimizedUnorderedChecksum = optimizedUnorderedChecksum;
            this.unorderedChecksumMatch = unorderedChecksumMatch;
            this.orderMatch = orderMatch;
        }

        public boolean isValid() { return valid; }
        public List<String> getErrors() { return errors; }
        public List<String> getWarnings() { return warnings; }
        public boolean isChecksumMatch() { return checksumMatch; }
        public boolean isUnorderedChecksumMatch() { return unorderedChecksumMatch; }
        public boolean isOrderMatch() { return orderMatch; }

        public String formatReport() {
            StringBuilder sb = new StringBuilder();
            sb.append("\n").append("-".repeat(60)).append("\n");
            sb.append("RESULT VALIDATION\n");
            sb.append("-".repeat(60)).append("\n");

            // Row counts
            sb.append(String.format("Original rows:    %d%n", originalRowCount));
            sb.append(String.format("Optimized rows:   %d%n", optimizedRowCount));
            sb.append(String.format("Row count match:  %s%n", originalRowCount == optimizedRowCount ? "YES" : "NO"));

            // Ordering
            sb.append(String.format("Order preserved:  %s%n", orderMatch ? "YES" : "NO"));

            // Checksums
            sb.append(String.format("Ordered checksum match:   %s%n", checksumMatch ? "YES" : "NO"));
            sb.append(String.format("  Original:   %s%n", originalChecksum));
            sb.append(String.format("  Optimized:  %s%n", optimizedChecksum));

            sb.append(String.format("Unordered checksum match: %s%n", unorderedChecksumMatch ? "YES" : "NO"));
            sb.append(String.format("  Original:   %s%n", originalUnorderedChecksum));
            sb.append(String.format("  Optimized:  %s%n", optimizedUnorderedChecksum));

            // Errors
            if (!errors.isEmpty()) {
                sb.append("\nERRORS:\n");
                for (String error : errors) {
                    sb.append("  - ").append(error).append("\n");
                }
            }

            // Warnings
            if (!warnings.isEmpty()) {
                sb.append("\nWARNINGS:\n");
                for (String warning : warnings) {
                    sb.append("  - ").append(warning).append("\n");
                }
            }

            // Overall result
            sb.append("\n");
            if (valid) {
                sb.append("VALIDATION: PASSED - Results are equivalent\n");
            } else if (unorderedChecksumMatch) {
                sb.append("VALIDATION: PASSED WITH WARNINGS - Content equivalent but order differs\n");
            } else {
                sb.append("VALIDATION: FAILED - Results are NOT equivalent\n");
            }

            return sb.toString();
        }
    }
}
