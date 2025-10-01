-- This script is a PostgreSQL translation of the original insert_observation_period.sql

INSERT INTO @cdm_schema.observation_period
(
	observation_period_id,
	person_id,
	observation_period_start_date,
	observation_period_end_date,
	period_type_concept_id
)
SELECT
	row_number()over(order by person_id) as observation_period_id,
	p.person_id,
	min(e.start) as observation_period_start_date,
	max(e.stop) as observation_period_end_date,
	44814724 as period_type_concept_id
FROM @cdm_schema.person p
JOIN @synthea_schema.encounters e
  ON p.person_source_value = e.patient
GROUP BY p.person_id;