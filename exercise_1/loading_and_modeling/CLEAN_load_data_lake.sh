# save my current working directory
MY_CWD=$(pwd)

# empty and remove staging directories
rm ~/staging/exercise_1/*
rmdir ~/staging/exercise_1
rmdir ~/staging

# remove files from hdfs
hdfs dfs -rm /user/w205/hospital_compare/hospitals/hospitals.csv
hdfs dfs -rm /user/w205/hospital_compare/survey_responses/survey_responses.csv
hdfs dfs -rm /user/w205/hospital_compare/effective_care/effective_care.csv
hdfs dfs -rm /user/w205/hospital_compare/measures/Measures.csv
hdfs dfs -rm /user/w205/hospital_compare/readmissions/readmissions.csv

# remove our hdfs directories
hdfs dfs -rmdir /user/w205/hospital_compare/hospitals
hdfs dfs -rmdir /user/w205/hospital_compare/survey_responses
hdfs dfs -rmdir /user/w205/hospital_compare/effective_care
hdfs dfs -rmdir /user/w205/hospital_compare/measures
hdfs dfs -rmdir /user/w205/hospital_compare/readmissions

# remove our hdfs directory
hdfs dfs -rmdir /user/w205/hospital_compare

# change directory back to the original
cd $MY_CWD

# clean exit
exit
