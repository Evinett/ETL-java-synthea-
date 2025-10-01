package com.example.syntheaetl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EtlRunner {

    // --- Configuration - Match this to your run_etl.R script ---
    private static final String CDM_SCHEMA = "cdm_synthea";
    private static final String ACHILLES_RESULTS_SCHEMA = "results_synthea";
    private static final String CDM_VERSION = "5.4"; // Target CDM version
    private static final String SYNTHEA_VERSION = "3.3.0"; // For cdm_source table
    private static final String SYNTHEA_SCHEMA = "native";
    private static final String SYNTHEA_CSV_PATH = "/Users/rward/Developer/csv";
    private static final String VOCAB_CSV_PATH = "/Users/rward/Developer/ETL-Java-Cursor-fix/CDM_may25";

    public static void main(String[] args) {
        boolean loadVocab = args.length > 0 && "--load-vocab".equalsIgnoreCase(args[0]);

        // Use try-with-resources on the DatabaseManager to ensure the connection pool is closed.
        try (DatabaseManager dbManager = new DatabaseManager()) {
            try (Connection conn = dbManager.getConnection()) {
                if (loadVocab) {
                    runVocabularyLoad(conn);
                } else {
                    runSyntheaEtl(conn);
                }
            }
        } catch (Exception e) {
            System.err.println("\nAn error occurred during the ETL process.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void runVocabularyLoad(Connection conn) throws Exception {
        System.out.println("--- Running ONE-TIME Vocabulary Load ---");
        // Step 1: Clean and create the CDM schema to ensure a fresh, idempotent run.
        System.out.println("Dropping and recreating CDM schema for a clean vocabulary load...");
        try (Statement stmt = conn.createStatement()) {
            // Set a timeout to prevent hanging on a locked schema. 30 seconds should be ample.
            stmt.setQueryTimeout(30);
            stmt.execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", CDM_SCHEMA));
            stmt.execute(String.format("CREATE SCHEMA %s", CDM_SCHEMA));
            // Also ensure the native and results schemas exist for other parts of the ETL.
            stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", SYNTHEA_SCHEMA));
            stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", ACHILLES_RESULTS_SCHEMA));
        }

        // Step 2: Create the empty OMOP CDM tables
        CdmManager cdmManager = new CdmManager(conn, CDM_SCHEMA);
        cdmManager.createCdmTables(CDM_VERSION, "postgresql");

        // Step 3: Load Vocabulary CSVs
        System.out.println("\n--- Loading Vocabulary CSVs ---");
        List<String> vocabFiles = List.of(
            "concept.csv", "vocabulary.csv", "concept_ancestor.csv",
            "concept_relationship.csv", "relationship.csv", "concept_synonym.csv",
            "domain.csv", "concept_class.csv", "drug_strength.csv",
            "source_to_concept_map.csv"
        );
        CsvLoader vocabLoader = new CsvLoader(conn, CDM_SCHEMA, '\t', "yyyyMMdd");
        vocabLoader.loadAllCsvInDirectory(VOCAB_CSV_PATH, vocabFiles);

        System.out.println("\nVocabulary loading completed successfully.");
        System.out.println("You do not need to run this step again unless you update your vocabulary files.");
    }

    private static void runSyntheaEtl(Connection conn) throws Exception {
        System.out.println("--- Running Synthea Data ETL ---");
        // Step 1: Clean up previous run data (leaves vocabulary intact)
        cleanupCdmEventTables(conn);
        cleanupNativeSchema(conn);
        cleanupAchillesResultsSchema(conn);

        // Step 2: Create the raw Synthea tables
        SqlFileRunner.runSqlFile(conn, "src/main/resources/sql/postgresql/create_synthea_tables.sql", Map.of("synthea_schema", SYNTHEA_SCHEMA));

        // Step 3: Load Synthea CSVs into the 'native' schema
        System.out.println("\n--- Loading Synthea CSVs ---");
        CsvLoader syntheaLoader = new CsvLoader(conn, SYNTHEA_SCHEMA, ',', "yyyy-MM-dd");
        syntheaLoader.loadAllCsvInDirectory(SYNTHEA_CSV_PATH, null);

        // Step 4 & 5: Create mapping tables and load final event tables
        SqlScriptManager scriptManager = new SqlScriptManager(conn, CDM_SCHEMA, SYNTHEA_SCHEMA, CDM_VERSION, SYNTHEA_VERSION);
        scriptManager.runMappingAndRollupScripts();
        scriptManager.runEventTableScripts();

        System.out.println("\nETL Process completed successfully.");
    }

    private static void createSchemas(Connection conn) throws SQLException {
        System.out.println("Creating schemas if they do not exist...");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", CDM_SCHEMA));
            stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", SYNTHEA_SCHEMA));
        }
        System.out.println("Schemas are ready.");
    }

    private static void cleanupNativeSchema(Connection conn) throws SQLException {
        System.out.println("Cleaning up native schema...");
        try (Statement stmt = conn.createStatement()) {
            // Set a timeout to prevent the application from hanging indefinitely on a locked schema.
            stmt.setQueryTimeout(30); // 30 seconds
            stmt.execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", SYNTHEA_SCHEMA));
            stmt.execute(String.format("CREATE SCHEMA %s", SYNTHEA_SCHEMA));
        }
        System.out.println("Native schema recreated.");
    }

    private static void cleanupAchillesResultsSchema(Connection conn) throws SQLException {
        System.out.println("Cleaning up Achilles results schema...");
        try (Statement stmt = conn.createStatement()) {
            // Set a timeout to prevent the application from hanging indefinitely on a locked schema.
            stmt.setQueryTimeout(30); // 30 seconds
            // Drop and recreate to ensure a clean slate for the new analysis
            stmt.execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", ACHILLES_RESULTS_SCHEMA));
            stmt.execute(String.format("CREATE SCHEMA %s", ACHILLES_RESULTS_SCHEMA));
        }
        System.out.println("Achilles results schema recreated.");
    }

    private static void cleanupCdmEventTables(Connection conn) throws SQLException {
        System.out.println("Truncating CDM event tables...");
        // List of tables that hold patient data and are safe to truncate
        // Vocabulary tables are NOT in this list.
        Set<String> tablesToTruncate = Set.of(
                "person", "observation_period", "visit_occurrence", "visit_detail",
                "condition_occurrence", "drug_exposure", "procedure_occurrence",
                "device_exposure", "measurement", "observation", "death", "note",
                "note_nlp", "specimen", "fact_relationship", "location", "care_site",
                "provider", "payer_plan_period", "cost", "drug_era", "dose_era",
                "condition_era", "cdm_source"
        );

        String sql = "TRUNCATE TABLE %s.%s RESTART IDENTITY CASCADE";

        try (Statement stmt = conn.createStatement()) {
            // Set a timeout on DDL statements to prevent the application from hanging on locks.
            stmt.setQueryTimeout(60); // 60 seconds, as TRUNCATE with CASCADE can be slower.
            // Temporarily disable triggers on all tables to speed up truncation with CASCADE
            stmt.execute("SET session_replication_role = 'replica';");

            for (String tableName : tablesToTruncate) {
                System.out.println(" - Truncating " + tableName);
                stmt.execute(String.format(sql, CDM_SCHEMA, tableName));
            }

            // Re-enable triggers
            stmt.execute("SET session_replication_role = 'origin';");
        }
        System.out.println("CDM event tables truncated successfully.");
    }
}
