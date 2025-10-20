-- This script is a PostgreSQL translation of the original create_source_to_concept_map.sql

DROP TABLE IF EXISTS @cdm_schema.source_to_concept_map;

CREATE TABLE @cdm_schema.source_to_concept_map AS
WITH all_source_codes AS (
    -- Conditions (Synthea uses http://snomed.info/sct)
    SELECT code AS source_code, 'SNOMED' AS source_vocabulary_id 
    FROM @synthea_schema.conditions 
    WHERE system LIKE '%snomed%'
    UNION
    -- Procedures (Synthea uses http://snomed.info/sct)
    SELECT code AS source_code, 'SNOMED' AS source_vocabulary_id 
    FROM @synthea_schema.procedures 
    WHERE system LIKE '%snomed%'
    UNION
    -- Medications (Synthea uses http://www.nlm.nih.gov/research/umls/rxnorm)
    SELECT code AS source_code, 'RxNorm' AS source_vocabulary_id 
    FROM @synthea_schema.medications
    UNION
    -- Observations (Synthea uses http://loinc.org)
    SELECT code AS source_code, 'LOINC' AS source_vocabulary_id 
    FROM @synthea_schema.observations
    UNION
    -- Devices (Synthea uses http://snomed.info/sct)
    SELECT code AS source_code, 'SNOMED' AS source_vocabulary_id 
    FROM @synthea_schema.devices
    UNION
    -- Allergies (Synthea uses http://snomed.info/sct)
    SELECT code AS source_code, 'SNOMED' AS source_vocabulary_id 
    FROM @synthea_schema.allergies 
    WHERE system LIKE '%snomed%'
)
SELECT
    DISTINCT s.source_code,
    s.source_vocabulary_id,
    c.concept_id as source_concept_id,
    c.domain_id as source_domain_id,
    coalesce(c2.concept_id, 0) as target_concept_id,
    c2.vocabulary_id as target_vocabulary_id,
    c2.domain_id as target_domain_id
FROM all_source_codes s
JOIN @cdm_schema.concept c ON s.source_code = c.concept_code AND s.source_vocabulary_id = c.vocabulary_id
LEFT JOIN @cdm_schema.concept_relationship cr ON c.concept_id = cr.concept_id_1 AND cr.relationship_id = 'Maps to' AND cr.invalid_reason IS NULL
LEFT JOIN @cdm_schema.concept c2 ON cr.concept_id_2 = c2.concept_id AND c2.invalid_reason IS NULL;