-- This script is a PostgreSQL translation of the original create_visit_rollup_tables.sql

-- Create a temp table to store the pre-processed visit data
DROP TABLE IF EXISTS temp_era_logic_pre;
CREATE TEMP TABLE temp_era_logic_pre AS
SELECT
    e.id AS encounter_id,
    e.patient AS person_source_value,
    e.start AS visit_start_date,
    e.stop AS visit_end_date,
    e.encounterclass AS visit_source_value,
    e.organization AS organization_source_value
FROM
    @synthea_schema.encounters e
WHERE
    e.start IS NOT NULL;

-- Create a temp table to apply the era-building logic
DROP TABLE IF EXISTS temp_era_logic_post;
CREATE TEMP TABLE temp_era_logic_post AS
WITH
ctePredecessor as
(
    SELECT
        *,
        LAG(visit_end_date, 1) OVER (PARTITION BY person_source_value, visit_source_value, organization_source_value ORDER BY visit_start_date) as predecessor_visit_end_date
    FROM
        temp_era_logic_pre
),
cteLag as
(
    SELECT
        *,
        (visit_start_date::date - predecessor_visit_end_date::date) as lag_time
    FROM
        ctePredecessor
),
cteStart as
(
    SELECT
        *,
        CASE
            WHEN lag_time <= 1 THEN 0
            ELSE 1
        END as start_event
    FROM
        cteLag
),
cteEpisode as
(
    SELECT
        *,
        SUM(start_event) OVER (PARTITION BY person_source_value, visit_source_value, organization_source_value ORDER BY visit_start_date) as episode
    FROM
        cteStart
)
SELECT
    encounter_id,
    person_source_value,
    visit_source_value,
    organization_source_value,
    episode,
    MIN(visit_start_date) as visit_start_date,
    MAX(visit_end_date) as visit_end_date
FROM
    cteEpisode
GROUP BY
    encounter_id, person_source_value, visit_source_value, organization_source_value, episode;

-- Create the final visit_occurrence_rollup table
DROP TABLE IF EXISTS @synthea_schema.visit_occurrence_rollup;
CREATE TABLE @synthea_schema.visit_occurrence_rollup AS
SELECT
    person_source_value,
    MIN(visit_start_date) as visit_start_date,
    MAX(visit_end_date) as visit_end_date,
    visit_source_value,
    organization_source_value
FROM
    temp_era_logic_post
GROUP BY
    person_source_value, visit_source_value, organization_source_value, episode;

-- Create the final visit_detail_rollup table
DROP TABLE IF EXISTS @synthea_schema.visit_detail_rollup;
CREATE TABLE @synthea_schema.visit_detail_rollup AS
SELECT
    elp.encounter_id,
    elp.person_source_value,
    elp.visit_start_date,
    elp.visit_end_date,
    elp.visit_source_value,
    elp.organization_source_value,
    vor.visit_start_date as visit_occurrence_start_date
FROM
    temp_era_logic_post elp
JOIN
    @synthea_schema.visit_occurrence_rollup vor
ON
    elp.person_source_value = vor.person_source_value
    AND elp.visit_source_value = vor.visit_source_value
    AND elp.organization_source_value = vor.organization_source_value
    AND elp.visit_start_date >= vor.visit_start_date
    AND elp.visit_start_date <= vor.visit_end_date;

-- Clean up temp tables
DROP TABLE IF EXISTS temp_era_logic_pre;
DROP TABLE IF EXISTS temp_era_logic_post;