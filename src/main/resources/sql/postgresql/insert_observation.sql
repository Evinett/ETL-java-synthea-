-- This script is a PostgreSQL translation of the original insert_observation.sql

INSERT INTO @cdm_schema.observation
(
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_string,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    visit_occurrence_id
)
SELECT
    row_number()over(order by p.person_id, o.date) as observation_id,
    p.person_id,
    COALESCE(stcm.target_concept_id, 0) AS observation_concept_id,
    o.date AS observation_date,
    o.date::timestamp AS observation_datetime,
    32817 AS observation_type_concept_id, -- EHR
    SUBSTRING(o.value, 1, 60) AS value_as_string,
    SUBSTRING(o.code, 1, 50) AS observation_source_value,
    COALESCE(stcm.source_concept_id, 0) AS observation_source_concept_id,
    SUBSTRING(o.units, 1, 50) AS unit_source_value,
    vd.visit_occurrence_id
FROM @synthea_schema.observations o
JOIN @cdm_schema.person p ON o.patient = p.person_source_value
LEFT JOIN @cdm_schema.source_to_concept_map stcm ON o.code = stcm.source_code AND stcm.source_vocabulary_id = 'LOINC'
LEFT JOIN @cdm_schema.visit_detail vd ON o.encounter = vd.visit_detail_source_value
WHERE o.type = 'text';