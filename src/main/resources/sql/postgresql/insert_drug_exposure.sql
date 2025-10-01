-- This script is a PostgreSQL translation of the original insert_drug_exposure.sql

INSERT INTO @cdm_schema.drug_exposure
(
    drug_exposure_id,
    person_id,
    drug_concept_id,
    drug_exposure_start_date,
    drug_exposure_start_datetime,
    drug_exposure_end_date,
    drug_exposure_end_datetime,
    drug_type_concept_id,
    drug_source_value,
    drug_source_concept_id,
    visit_occurrence_id
)
SELECT
    row_number()over(order by p.person_id, m.start) as drug_exposure_id,
    p.person_id,
    COALESCE(stcm.target_concept_id, 0) AS drug_concept_id,
    m.start AS drug_exposure_start_date,
    m.start::timestamp AS drug_exposure_start_datetime,
    COALESCE(m.stop, m.start) AS drug_exposure_end_date,
    COALESCE(m.stop, m.start)::timestamp AS drug_exposure_end_datetime,
    38000177 AS drug_type_concept_id, -- Prescription written
    m.code AS drug_source_value,
    COALESCE(stcm.source_concept_id, 0) AS drug_source_concept_id,
    vd.visit_occurrence_id
FROM @synthea_schema.medications m
JOIN @cdm_schema.person p ON m.patient = p.person_source_value
LEFT JOIN @cdm_schema.source_to_concept_map stcm ON m.code = stcm.source_code AND stcm.source_vocabulary_id = 'RxNorm'
LEFT JOIN @cdm_schema.visit_detail vd ON m.encounter = vd.visit_detail_source_value;
