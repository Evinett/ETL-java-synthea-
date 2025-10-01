-- Simplified condition era creation
INSERT INTO @cdm_schema.condition_era (
	condition_era_id,
	person_id,
	condition_concept_id,
	condition_era_start_date,
	condition_era_end_date,
	condition_occurrence_count
	)
SELECT 
	row_number() OVER (ORDER BY person_id, condition_concept_id, min_start_date) AS condition_era_id,
	person_id,
	condition_concept_id,
	min_start_date AS condition_era_start_date,
	max_end_date AS condition_era_end_date,
	condition_occurrence_count
FROM (
	SELECT 
		person_id,
		condition_concept_id,
		MIN(condition_start_date) AS min_start_date,
		MAX(COALESCE(condition_end_date, condition_start_date)) AS max_end_date,
		COUNT(*) AS condition_occurrence_count
	FROM @cdm_schema.condition_occurrence
	WHERE condition_concept_id != 0
	GROUP BY person_id, condition_concept_id
) grouped_conditions;