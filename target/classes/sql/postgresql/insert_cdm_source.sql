-- This script populates the cdm_source table with metadata about the ETL run.

INSERT INTO @cdm_schema.cdm_source
(
    cdm_source_name,
    cdm_source_abbreviation,
    cdm_holder,
    source_description,
    source_documentation_reference,
    cdm_etl_reference,
    source_release_date,
    cdm_release_date,
    cdm_version,
    vocabulary_version,
    cdm_version_concept_id
)
VALUES
(
    'Synthea synthetic health database',
    'Synthea',
    'OHDSI',
    'SyntheaTM is a Synthetic Patient Population Simulator. The goal is to output synthetic, realistic (but not real), patient data and associated health records in a variety of formats.',
    'https://github.com/synthetichealth/synthea',
    'https://github.com/OHDSI/ETL-Synthea',
    CURRENT_DATE, -- Using current date as source release date
    CURRENT_DATE, -- Using current date as CDM release date
    '@cdm_version', -- This will be replaced by the Java code
    COALESCE((SELECT vocabulary_version FROM @cdm_schema.vocabulary WHERE vocabulary_id = 'None'), 'Version unknown'),
    COALESCE((SELECT concept_id FROM @cdm_schema.concept WHERE concept_name = 'OMOP CDM v' || '@cdm_version' AND vocabulary_id = 'CDM'), 756265)
);

-- Note: The above query assumes a concept_name like 'OMOP CDM v5.4.0' exists in your CONCEPT table.
-- You may need to adjust the string concatenation depending on the exact format in the vocabulary.
-- For example, if the concept name is just '5.4.0', the WHERE clause would be different.