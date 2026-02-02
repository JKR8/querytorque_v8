package com.qtcalcite.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
        name = "qtcalcite",
        description = "LLM-R2 Query Optimizer using Apache Calcite and DuckDB",
        mixinStandardHelpOptions = true,
        version = "QTCalcite 1.0.0",
        subcommands = {
                ManualCommand.class,
                AutoCommand.class,
                TpcdsBenchmarkCommand.class,
                CommandLine.HelpCommand.class
        }
)
public class MainCommand implements Runnable {

    @Option(names = {"-c", "--config"}, description = "Configuration file path")
    String configPath;

    @Option(names = {"-d", "--database"}, description = "DuckDB database path")
    String databasePath;

    @Option(names = {"-v", "--verbose"}, description = "Enable verbose output")
    boolean verbose;

    @Override
    public void run() {
        // If no subcommand specified, show help
        CommandLine.usage(this, System.out);
    }

    public String getConfigPath() { return configPath; }
    public String getDatabasePath() { return databasePath; }
    public boolean isVerbose() { return verbose; }
}
