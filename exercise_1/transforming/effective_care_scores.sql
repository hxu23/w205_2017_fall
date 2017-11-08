DROP TABLE IF EXISTS effective_care_scores;
 
CREATE TABLE effective_care_scores as 
SELECT 
       provider_id,
       measure_id,
       measure_name,
       condition,
       measure_start_date, 
       measure_end_date, 
       cast(score as decimal(1,0)) score FROM effective_care
WHERE measure_id not like 'EDV%' AND score not like 'Not%';  
