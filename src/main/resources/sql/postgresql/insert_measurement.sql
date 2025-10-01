-- This script is a PostgreSQL translation of the original insert_measurement.sql

INSERT INTO @cdm_schema.measurement
(
    measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_type_concept_id,
    value_as_number,
    value_source_value,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    visit_occurrence_id
)
SELECT
    row_number()over(order by p.person_id, o.date) as measurement_id,
    p.person_id,
    COALESCE(stcm.target_concept_id, 0) AS measurement_concept_id,
    o.date AS measurement_date,
    o.date::timestamp AS measurement_datetime,
    32817 AS measurement_type_concept_id, -- EHR
    CAST(o.value AS numeric) AS value_as_number,
    o.value AS value_source_value,
    o.code AS measurement_source_value,
    COALESCE(stcm.source_concept_id, 0) AS measurement_source_concept_id,
    o.units AS unit_source_value,
    vd.visit_occurrence_id
FROM @synthea_schema.observations o
JOIN @cdm_schema.person p ON o.patient = p.person_source_value
LEFT JOIN @cdm_schema.source_to_concept_map stcm ON o.code = stcm.source_code AND stcm.source_vocabulary_id = 'LOINC'
LEFT JOIN @cdm_schema.visit_detail vd ON o.encounter = vd.visit_detail_source_value
WHERE o.type = 'numeric';