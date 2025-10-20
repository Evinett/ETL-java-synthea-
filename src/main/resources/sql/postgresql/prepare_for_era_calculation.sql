-- Add indexes to improve era calculation performance on large datasets
-- These indexes significantly speed up the window functions and joins

-- Drug exposure indexes
CREATE INDEX IF NOT EXISTS idx_drug_exp_person_ingredient_date 
ON @cdm_schema.drug_exposure(person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date);

CREATE INDEX IF NOT EXISTS idx_drug_exp_concept_id 
ON @cdm_schema.drug_exposure(drug_concept_id) WHERE drug_concept_id != 0;

-- Condition occurrence indexes  
CREATE INDEX IF NOT EXISTS idx_cond_occ_person_concept_date
ON @cdm_schema.condition_occurrence(person_id, condition_concept_id, condition_start_date, condition_end_date);

-- Concept ancestor indexes (if not already present)
CREATE INDEX IF NOT EXISTS idx_concept_ancestor_desc 
ON @cdm_schema.concept_ancestor(descendant_concept_id, ancestor_concept_id);

CREATE INDEX IF NOT EXISTS idx_concept_ancestor_anc
ON @cdm_schema.concept_ancestor(ancestor_concept_id);

-- Update statistics
ANALYZE @cdm_schema.drug_exposure;
ANALYZE @cdm_schema.condition_occurrence;
ANALYZE @cdm_schema.concept_ancestor;
ANALYZE @cdm_schema.concept;