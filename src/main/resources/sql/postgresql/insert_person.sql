-- This script is a PostgreSQL translation of the original insert_person.sql

INSERT INTO @cdm_schema.person
(
	person_id,
	gender_concept_id,
	year_of_birth,
	month_of_birth,
	day_of_birth,
	race_concept_id,
	ethnicity_concept_id,
	location_id,
	person_source_value,
	gender_source_value,
	race_source_value,
	race_source_concept_id,
	ethnicity_source_value,
	ethnicity_source_concept_id
)
SELECT
	row_number()over(order by p.id) as person_id,
	CASE
		WHEN p.gender = 'M' THEN 8507
		WHEN p.gender = 'F' THEN 8532
	END as gender_concept_id,
	EXTRACT(YEAR  FROM p.birthdate) as year_of_birth,
	EXTRACT(MONTH FROM p.birthdate) as month_of_birth,
	EXTRACT(DAY   FROM p.birthdate) as day_of_birth,
	CASE
		WHEN p.race = 'white' THEN 8527
		WHEN p.race = 'black' THEN 8516
		WHEN p.race = 'asian' THEN 8515
		ELSE 0
	END as race_concept_id,
	CASE
		WHEN p.ethnicity = 'hispanic' THEN 38003563
		WHEN p.ethnicity = 'non-hispanic' THEN 38003564
		ELSE 0
	END as ethnicity_concept_id,
	l.location_id as location_id,
	p.id as person_source_value,
	p.gender as gender_source_value,
	p.race as race_source_value,
	0 as race_source_concept_id,
	p.ethnicity as ethnicity_source_value,
	0 as ethnicity_source_concept_id
FROM @synthea_schema.patients p
JOIN @cdm_schema.location l
  ON p.id = l.location_source_value
WHERE p.gender is not null;