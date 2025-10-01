-- This script is a PostgreSQL translation of the original insert_condition_occurrence.sql

INSERT INTO @cdm_schema.condition_occurrence
(
    condition_occurrence_id,
    person_id,
    condition_concept_id,
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_end_datetime,
    condition_type_concept_id,
    condition_source_value,
    condition_source_concept_id,
    visit_occurrence_id
)
SELECT
    row_number()over(order by p.person_id, c.start) as condition_occurrence_id,
    p.person_id,
    COALESCE(stcm.target_concept_id, 0) AS condition_concept_id,
    c.start AS condition_start_date,
    c.start::timestamp AS condition_start_datetime,
    c.stop AS condition_end_date,
    c.stop::timestamp AS condition_end_datetime,
    32020 AS condition_type_concept_id, -- EHR Encounter Diagnosis
    c.code AS condition_source_value,
    COALESCE(stcm.source_concept_id, 0) AS condition_source_concept_id,
    vd.visit_occurrence_id
FROM @synthea_schema.conditions c
JOIN @cdm_schema.person p ON c.patient = p.person_source_value
LEFT JOIN @cdm_schema.source_to_concept_map stcm ON c.code = stcm.source_code AND stcm.source_vocabulary_id = 'SNOMED'
LEFT JOIN @cdm_schema.visit_detail vd ON c.encounter = vd.visit_detail_source_value;