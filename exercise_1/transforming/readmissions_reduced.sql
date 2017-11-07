DROP TABLE IF EXISTS readmissions_reduced;
 
CREATE TABLE readmissions_reduced as 
SELECT
	provider_id,
	measure_name,
	measure_id,
	vha_national_rate,
	compare_to_national,
        cast(score as decimal(1,0)) score FROM readmissions
WHERE score not like "Not%";
