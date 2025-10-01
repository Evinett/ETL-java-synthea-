-- This script is a PostgreSQL translation of the original insert_visit_detail.sql

INSERT INTO @cdm_schema.visit_detail
(
	visit_detail_id,
	person_id,
	visit_detail_concept_id,
	visit_detail_start_date,
	visit_detail_end_date,
	visit_detail_type_concept_id,
	care_site_id,
	visit_detail_source_value,
	visit_occurrence_id
)
SELECT
	row_number()over(order by p.person_id, vdr.visit_start_date) as visit_detail_id,
	p.person_id,
	CASE
		WHEN vdr.visit_source_value = 'inpatient' THEN 9201
		WHEN vdr.visit_source_value = 'outpatient' THEN 9202
		WHEN vdr.visit_source_value = 'emergency' THEN 9203
		ELSE 0
	END as visit_detail_concept_id,
	vdr.visit_start_date,
	vdr.visit_end_date,
	44818517 as visit_detail_type_concept_id,
	cs.care_site_id,
	vdr.encounter_id as visit_detail_source_value,
	vo.visit_occurrence_id
FROM @synthea_schema.visit_detail_rollup vdr
JOIN @cdm_schema.person p
  ON vdr.person_source_value = p.person_source_value
JOIN @cdm_schema.visit_occurrence vo
  ON vdr.visit_occurrence_start_date = vo.visit_start_date AND p.person_id = vo.person_id
LEFT JOIN @cdm_schema.care_site cs
  ON vdr.organization_source_value = cs.care_site_source_value;