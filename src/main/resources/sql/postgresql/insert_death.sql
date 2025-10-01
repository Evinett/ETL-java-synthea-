-- This script is a PostgreSQL translation of the original insert_death.sql

INSERT INTO @cdm_schema.death
(
    person_id,
    death_date,
    death_datetime,
    death_type_concept_id
)
SELECT
    p.person_id,
    pat.deathdate AS death_date,
    pat.deathdate::timestamp AS death_datetime,
    32817 AS death_type_concept_id -- EHR
FROM @synthea_schema.patients pat
JOIN @cdm_schema.person p
  ON pat.id = p.person_source_value
WHERE pat.deathdate IS NOT NULL;