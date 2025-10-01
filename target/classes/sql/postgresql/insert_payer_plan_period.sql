-- This script is a PostgreSQL translation of the original insert_payer_plan_period.sql

INSERT INTO @cdm_schema.payer_plan_period
(
    payer_plan_period_id,
    person_id,
    payer_plan_period_start_date,
    payer_plan_period_end_date,
    payer_source_value,
    plan_source_value,
    family_source_value
)
SELECT
    row_number()over(order by p.person_id, pt.start_date) as payer_plan_period_id,
    p.person_id,
    pt.start_date AS payer_plan_period_start_date,
    pt.end_date AS payer_plan_period_end_date,
    pt.payer AS payer_source_value,
    pt.plan_ownership AS plan_source_value,
    pt.owner_name AS family_source_value
FROM @synthea_schema.payer_transitions pt
JOIN @cdm_schema.person p ON pt.patient = p.person_source_value;