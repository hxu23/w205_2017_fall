DROP TABLE IF EXISTS hospitals_reduced;

CREATE TABLE hospitals_reduced as 
SELECT 
       provider_id,
       hospital_name,
       city,
       state,
       zip_code,
       county_name,
       hospital_type,
       hospital_ownership,
       meets_criteria,
       hospital_overall_rating,
       mortality_national_compare,
       safety_of_care_national_comparison,
       readmission_national_comparison,
       patient_experience_national_comparison,
       effectiveness_of_care_national_comparison,
       timeliness_of_care_national_comparison
FROM hospitals;

