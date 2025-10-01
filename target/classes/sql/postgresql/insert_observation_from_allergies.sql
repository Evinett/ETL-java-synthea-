-- This script loads allergy data into the observation table.
-- Allergies are recorded as observations in OMOP.

INSERT INTO @cdm_schema.observation
(
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    observation_source_value,
    observation_source_concept_id,
    visit_occurrence_id
)
SELECT
    (SELECT COALESCE(MAX(observation_id), 0) FROM @cdm_schema.observation) + row_number()over(order by p.person_id, a.start) as observation_id,
    p.person_id,
    COALESCE(stcm.target_concept_id, 0) AS observation_concept_id,
    a.start AS observation_date,
    a.start::timestamp AS observation_datetime,
    32817 AS observation_type_concept_id, -- EHR
    a.code AS observation_source_value,
    COALESCE(stcm.source_concept_id, 0) AS observation_source_concept_id,
    vd.visit_occurrence_id
FROM @synthea_schema.allergies a
JOIN @cdm_schema.person p ON a.patient = p.person_source_value
LEFT JOIN @cdm_schema.source_to_concept_map stcm ON a.code = stcm.source_code AND stcm.source_vocabulary_id = 'SNOMED'
LEFT JOIN @cdm_schema.visit_detail vd ON a.encounter = vd.visit_detail_source_value;
