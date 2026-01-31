package com.qtcalcite;

import com.qtcalcite.cli.MainCommand;
import picocli.CommandLine;

/**
 * QTCalcite - LLM-R2 Query Optimizer using Apache Calcite and DuckDB.
 *
 * This tool implements the LLM-R2 methodology: using LLMs to select
 * Apache Calcite rewrite rules for query optimization, executed against DuckDB.
 */
public class QTCalcite {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new MainCommand())
                .setCaseInsensitiveEnumValuesAllowed(true)
                .execute(args);
        System.exit(exitCode);
    }
}
