-- This script is a PostgreSQL translation of the original insert_procedure_occurrence.sql

INSERT INTO @cdm_schema.procedure_occurrence
(
    procedure_occurrence_id,
    person_id,
    procedure_concept_id,
    procedure_date,
    procedure_datetime,
    procedure_type_concept_id,
    procedure_source_value,
    procedure_source_concept_id,
    visit_occurrence_id
)
SELECT
    row_number()over(order by p.person_id, pr.start) as procedure_occurrence_id,
    p.person_id,
    COALESCE(stcm.target_concept_id, 0) AS procedure_concept_id,
    pr.start AS procedure_date,
    pr.start::timestamp AS procedure_datetime,
    32817 AS procedure_type_concept_id, -- EHR
    pr.code AS procedure_source_value,
    COALESCE(stcm.source_concept_id, 0) AS procedure_source_concept_id,
    vd.visit_occurrence_id
FROM @synthea_schema.procedures pr
JOIN @cdm_schema.person p ON pr.patient = p.person_source_value
LEFT JOIN @cdm_schema.source_to_concept_map stcm ON pr.code = stcm.source_code AND stcm.source_vocabulary_id = 'SNOMED'
LEFT JOIN @cdm_schema.visit_detail vd ON pr.encounter = vd.visit_detail_source_value;