-- This script is a PostgreSQL translation of the original insert_visit_occurrence.sql

INSERT INTO @cdm_schema.visit_occurrence
(
	visit_occurrence_id,
	person_id,
	visit_concept_id,
	visit_start_date,
	visit_end_date,
	visit_type_concept_id,
	care_site_id,
	visit_source_value
)
SELECT
	row_number()over(order by p.person_id, vr.visit_start_date) as visit_occurrence_id,
	p.person_id,
	CASE
		WHEN vr.visit_source_value = 'inpatient' THEN 9201
		WHEN vr.visit_source_value = 'outpatient' THEN 9202
		WHEN vr.visit_source_value = 'emergency' THEN 9203
		ELSE 0
	END as visit_concept_id,
	vr.visit_start_date,
	vr.visit_end_date,
	44818517 as visit_type_concept_id,
	cs.care_site_id,
	vr.visit_source_value
FROM @synthea_schema.visit_occurrence_rollup vr
JOIN @cdm_schema.person p
  ON vr.person_source_value = p.person_source_value
LEFT JOIN @cdm_schema.care_site cs
  ON vr.organization_source_value = cs.care_site_source_value;