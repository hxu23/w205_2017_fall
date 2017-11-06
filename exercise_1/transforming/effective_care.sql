DROP TABLE IF EXISTS effective_care_scores;
 
CREATE TABLE effective_care_scores as 
SELECT measure_id,
       measure_name,
       hospital_name, 
       state, 
       cast(score as decimal(1,0)) score FROM effective_care
WHERE measure_id not like 'EDV%' AND score not like 'Not%';  
