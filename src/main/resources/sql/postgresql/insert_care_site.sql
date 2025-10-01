-- This script is a PostgreSQL translation of the original insert_care_site.sql

INSERT INTO @cdm_schema.care_site
(
	care_site_id,
	care_site_name,
	place_of_service_concept_id,
	location_id,
	care_site_source_value
)
SELECT
	row_number()over(order by o.id) as care_site_id,
	o.name as care_site_name,
	NULL as place_of_service_concept_id,
	l.location_id as location_id,
	o.id as care_site_source_value
FROM @synthea_schema.organizations o
LEFT JOIN @cdm_schema.location l
  ON o.id = l.location_source_value;