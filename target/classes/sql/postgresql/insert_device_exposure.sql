-- This script is a PostgreSQL translation of the original insert_device_exposure.sql

INSERT INTO @cdm_schema.device_exposure
(
    device_exposure_id,
    person_id,
    device_concept_id,
    device_exposure_start_date,
    device_exposure_start_datetime,
    device_exposure_end_date,
    device_exposure_end_datetime,
    device_type_concept_id,
    unique_device_id,
    device_source_value,
    device_source_concept_id,
    visit_occurrence_id
)
SELECT
    row_number()over(order by p.person_id, d.start) as device_exposure_id,
    p.person_id,
    COALESCE(stcm.target_concept_id, 0) AS device_concept_id,
    d.start AS device_exposure_start_date,
    d.start::timestamp AS device_exposure_start_datetime,
    d.stop AS device_exposure_end_date,
    d.stop::timestamp AS device_exposure_end_datetime,
    44818707 AS device_type_concept_id, -- EHR Detail
    d.udi AS unique_device_id,
    d.code AS device_source_value,
    COALESCE(stcm.source_concept_id, 0) AS device_source_concept_id,
    vd.visit_occurrence_id
FROM @synthea_schema.devices d
JOIN @cdm_schema.person p ON d.patient = p.person_source_value
LEFT JOIN @cdm_schema.source_to_concept_map stcm ON d.code = stcm.source_code AND stcm.source_vocabulary_id = 'SNOMED'
LEFT JOIN @cdm_schema.visit_detail vd ON d.encounter = vd.visit_detail_source_value;