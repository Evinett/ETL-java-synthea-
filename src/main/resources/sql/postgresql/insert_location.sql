-- This script is a PostgreSQL translation of the original insert_location.sql

INSERT INTO @cdm_schema.location
(
	location_id,
	address_1,
	city,
	state,
	zip,
	location_source_value
)
SELECT
	row_number()over(order by p.address) as location_id,
	p.address as address_1,
	p.city as city,
	s.state_abbreviation as state,
	p.zip as zip,
	p.id as location_source_value
FROM @synthea_schema.patients p
LEFT JOIN @cdm_schema.states_map s
  ON p.state = s.state;