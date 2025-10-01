package com.example.syntheaetl;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class SqlScriptManager {

    private final Connection connection;
    private final String cdmSchema;
    private final String syntheaSchema;
    private final String cdmVersion;
    private final String syntheaVersion;

    public SqlScriptManager(Connection connection, String cdmSchema, String syntheaSchema, String cdmVersion, String syntheaVersion) {
        this.connection = connection;
        this.cdmSchema = cdmSchema;
        this.syntheaSchema = syntheaSchema;
        this.cdmVersion = cdmVersion;
        this.syntheaVersion = syntheaVersion;
    }

    /**
     * Runs the SQL scripts that create the intermediate mapping and rollup tables.
     * This corresponds to the CreateMapAndRollupTables.r script.
     */
    public void runMappingAndRollupScripts() {
        System.out.println("\n--- Running Mapping and Rollup Scripts ---");
        Map<String, String> replacements = Map.of(
            "cdm_schema", this.cdmSchema,
            "synthea_schema", this.syntheaSchema
        );

        // Create the vocabulary mapping tables first, as they are needed by other steps.
        SqlFileRunner.runSqlFile(connection, "src/main/resources/sql/postgresql/create_source_to_concept_map.sql", replacements);
        SqlFileRunner.runSqlFile(connection, "src/main/resources/sql/postgresql/create_states_map.sql", replacements);
        SqlFileRunner.runSqlFile(connection, "src/main/resources/sql/postgresql/create_visit_rollup_tables.sql", replacements);
        System.out.println("--- Finished Mapping and Rollup Scripts ---\n");
    }

    /**
     * Runs the SQL scripts that load the final event tables from the raw data.
     * This corresponds to the LoadEventTables.r script.
     */
    public void runEventTableScripts() {
        System.out.println("\n--- Running Event Table Load Scripts ---");
        List<String> eventScripts = List.of(
            "insert_location.sql", "insert_care_site.sql", "insert_person.sql",
            "insert_observation_period.sql", "insert_provider.sql", "insert_visit_occurrence.sql",
            "insert_visit_detail.sql", "insert_condition_occurrence.sql", "insert_observation.sql",
            "insert_observation_from_allergies.sql", "insert_measurement.sql",
            "insert_procedure_occurrence.sql", "insert_drug_exposure.sql", "insert_device_exposure.sql",
            "insert_death.sql", "insert_payer_plan_period.sql", "insert_cost.sql", "insert_cdm_source.sql"
        );

        Map<String, String> replacements = Map.of(
            "cdm_schema", this.cdmSchema,
            "synthea_schema", this.syntheaSchema,
            "cdm_version", this.cdmVersion,
            "synthea_version", this.syntheaVersion
        );

        // Loop through and run the translated SQL files
        for (String script : eventScripts) {
            SqlFileRunner.runSqlFile(connection, "src/main/resources/sql/postgresql/" + script, replacements);
        }

        // Run the era-building scripts after the main event tables are populated.
        // Era scripts only need the cdm_schema
        Map<String, String> eraReplacements = Map.of("cdm_schema", this.cdmSchema);
        SqlFileRunner.runSqlFile(connection, "src/main/resources/sql/postgresql/insert_condition_era.sql", eraReplacements);
        SqlFileRunner.runSqlFile(connection, "src/main/resources/sql/postgresql/insert_drug_era.sql", eraReplacements);

        System.out.println("--- Finished Event Table Load Scripts ---\n");
    }
}