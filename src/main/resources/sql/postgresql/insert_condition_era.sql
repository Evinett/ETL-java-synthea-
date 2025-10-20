-- Condition Era with 30-day gap logic (standard OMOP approach)
-- Groups condition occurrences into continuous treatment periods
-- A new era starts when there's a gap > 30 days between condition end and next condition start

DROP TABLE IF EXISTS temp_condition_era;

CREATE TEMP TABLE temp_condition_era AS
WITH condition_dates AS (
    -- Get all condition occurrences with their dates
    SELECT
        person_id,
        condition_concept_id,
        condition_start_date,
        COALESCE(condition_end_date, condition_start_date) AS condition_end_date
    FROM @cdm_schema.condition_occurrence
    WHERE condition_concept_id != 0
),
condition_era_events AS (
    -- Create start and end events for each condition occurrence
    SELECT
        person_id,
        condition_concept_id,
        condition_start_date AS event_date,
        -1 AS event_type, -- start event
        ROW_NUMBER() OVER (
            PARTITION BY person_id, condition_concept_id 
            ORDER BY condition_start_date
        ) AS start_ordinal
    FROM condition_dates
    
    UNION ALL
    
    SELECT
        person_id,
        condition_concept_id,
        condition_end_date + INTERVAL '30 days' AS event_date, -- 30-day gap
        1 AS event_type, -- end event (with 30-day grace period)
        NULL AS start_ordinal
    FROM condition_dates
),
condition_era_endpoints AS (
    -- Find era endpoints (where running count of starts minus ends hits zero)
    SELECT
        person_id,
        condition_concept_id,
        event_date - INTERVAL '30 days' AS era_end_date -- subtract back the 30-day grace period
    FROM (
        SELECT
            person_id,
            condition_concept_id,
            event_date,
            event_type,
            MAX(start_ordinal) OVER (
                PARTITION BY person_id, condition_concept_id 
                ORDER BY event_date, event_type 
                ROWS UNBOUNDED PRECEDING
            ) AS start_ordinal,
            ROW_NUMBER() OVER (
                PARTITION BY person_id, condition_concept_id 
                ORDER BY event_date, event_type
            ) AS overall_ord
        FROM condition_era_events
    ) ordered_events
    WHERE (2 * start_ordinal) - overall_ord = 0
),
condition_eras AS (
    -- Match each condition start with its era end date
    SELECT
        cd.person_id,
        cd.condition_concept_id,
        cd.condition_start_date AS era_start_date,
        MIN(ce.era_end_date) AS era_end_date
    FROM condition_dates cd
    JOIN condition_era_endpoints ce 
        ON cd.person_id = ce.person_id 
        AND cd.condition_concept_id = ce.condition_concept_id
        AND ce.era_end_date >= cd.condition_start_date
    GROUP BY cd.person_id, cd.condition_concept_id, cd.condition_start_date
)
-- Aggregate to final era level with occurrence count
SELECT
    ROW_NUMBER() OVER (ORDER BY person_id, condition_concept_id, era_start_date) AS condition_era_id,
    person_id,
    condition_concept_id,
    era_start_date AS condition_era_start_date,
    era_end_date AS condition_era_end_date,
    COUNT(*) AS condition_occurrence_count
FROM condition_eras
GROUP BY person_id, condition_concept_id, era_start_date, era_end_date;

-- Insert into final table
INSERT INTO @cdm_schema.condition_era (
    condition_era_id,
    person_id,
    condition_concept_id,
    condition_era_start_date,
    condition_era_end_date,
    condition_occurrence_count
)
SELECT * FROM temp_condition_era;

DROP TABLE IF EXISTS temp_condition_era;