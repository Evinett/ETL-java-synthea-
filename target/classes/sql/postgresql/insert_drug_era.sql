-- This script is a PostgreSQL translation of the original drug era script

DROP TABLE IF EXISTS temp_drug_era;

CREATE TEMP TABLE temp_drug_era AS
WITH
ctePreDrugTarget AS (
    SELECT
        d.drug_exposure_id,
        d.person_id,
        c.concept_id AS ingredient_concept_id,
        d.drug_exposure_start_date,
        d.days_supply,
        COALESCE(
            d.drug_exposure_end_date,
            d.drug_exposure_start_date + (d.days_supply * INTERVAL '1 day'),
            d.drug_exposure_start_date + INTERVAL '1 day'
        ) AS drug_exposure_end_date
    FROM @cdm_schema.drug_exposure d
    JOIN @cdm_schema.concept_ancestor ca ON ca.descendant_concept_id = d.drug_concept_id
    JOIN @cdm_schema.concept c ON ca.ancestor_concept_id = c.concept_id
    WHERE c.vocabulary_id = 'RxNorm' AND c.concept_class_id = 'Ingredient'
      AND d.drug_concept_id != 0 AND COALESCE(d.days_supply, 0) >= 0
),
cteSubExposureEndDates AS (
    SELECT person_id, ingredient_concept_id, event_date AS end_date
    FROM (
        SELECT person_id, ingredient_concept_id, event_date, event_type,
               MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal,
               ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord
        FROM (
            SELECT person_id, ingredient_concept_id, drug_exposure_start_date AS event_date, -1 AS event_type,
                   ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_exposure_start_date) AS start_ordinal
            FROM ctePreDrugTarget
            UNION ALL
            SELECT person_id, ingredient_concept_id, drug_exposure_end_date, 1 AS event_type, NULL
            FROM ctePreDrugTarget
        ) RAWDATA
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteDrugExposureEnds AS (
    SELECT dt.person_id, dt.ingredient_concept_id, dt.drug_exposure_start_date, MIN(e.end_date) AS drug_sub_exposure_end_date
    FROM ctePreDrugTarget dt
    JOIN cteSubExposureEndDates e ON dt.person_id = e.person_id AND dt.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= dt.drug_exposure_start_date
    GROUP BY dt.drug_exposure_id, dt.person_id, dt.ingredient_concept_id, dt.drug_exposure_start_date
),
cteFinalTarget AS (
    SELECT person_id, ingredient_concept_id, drug_exposure_start_date, drug_sub_exposure_end_date,
           COUNT(*) as drug_exposure_count,
           (drug_sub_exposure_end_date::date - drug_exposure_start_date::date) as days_exposed
    FROM cteDrugExposureEnds
    GROUP BY person_id, ingredient_concept_id, drug_exposure_start_date, drug_sub_exposure_end_date
),
cteEndDates AS (
    SELECT person_id, ingredient_concept_id, (event_date - INTERVAL '30 day') AS end_date
    FROM (
        SELECT person_id, ingredient_concept_id, event_date, event_type,
               MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal,
               ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord
        FROM (
            SELECT person_id, ingredient_concept_id, drug_exposure_start_date AS event_date, -1 AS event_type,
                   ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_exposure_start_date) AS start_ordinal
            FROM cteFinalTarget
            UNION ALL
            SELECT person_id, ingredient_concept_id, (drug_sub_exposure_end_date + INTERVAL '30 day'), 1 AS event_type, NULL
            FROM cteFinalTarget
        ) RAWDATA
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
)
SELECT row_number() over (order by ft.person_id) as drug_era_id,
       ft.person_id, ft.ingredient_concept_id as drug_concept_id,
       min(drug_exposure_start_date) as drug_era_start_date,
       min(e.end_date) as drug_era_end_date,
       sum(drug_exposure_count) as drug_exposure_count,
       (min(e.end_date)::date - min(drug_exposure_start_date)::date) - sum(days_exposed) as gap_days
FROM cteFinalTarget ft
JOIN cteEndDates e ON ft.person_id = e.person_id AND ft.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= ft.drug_exposure_start_date
GROUP BY ft.person_id, ft.ingredient_concept_id;

INSERT INTO @cdm_schema.drug_era(drug_era_id, person_id, drug_concept_id, drug_era_start_date, drug_era_end_date, drug_exposure_count, gap_days)
SELECT * FROM temp_drug_era;

DROP TABLE IF EXISTS temp_drug_era;