-- This script is a PostgreSQL translation of the original insert_provider.sql

INSERT INTO @cdm_schema.provider
(
	provider_id,
	provider_name,
	care_site_id,
	provider_source_value,
	specialty_source_value,
	gender_source_value
)
SELECT
	row_number()over(order by p.id) as provider_id,
	p.name as provider_name,
	cs.care_site_id as care_site_id,
	p.id as provider_source_value,
	p.speciality as specialty_source_value,
	p.gender as gender_source_value
FROM @synthea_schema.providers p
LEFT JOIN @cdm_schema.care_site cs
  ON p.organization = cs.care_site_source_value;