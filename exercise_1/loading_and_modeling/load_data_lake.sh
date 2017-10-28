#Save my current directory
MY_CWD=$(pwd)

# creating staging directories
mkdir ~/staging
mkdir ~/staging/exercise_1

# change to staging directory
cd ~/staging/exercise_1

# get file from data.medicare.gov
MY_URL="https://data.medicare.gov/views/bg9k-emty/files/e514828f-8ed2-445f-b49f-5ac11a58869d?content_type=application%2Fzip%3B%20charset%3Dbinary&filename=Hospital_Revised_Flatfiles.zip"

wget "$MY_URL" -O medicare_data.zip

# unzip the medicare data
unzip medicare_data.zip

# create our hfs directory
hdfs dfs -mkdir /user/w205/hospital_compare

# remove first line of files and rename

OLD_FILE="Hospital General Information.csv"
NEW_FILE="hospitals.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
# copy the files to hfds
hdfs dfs -mkdir /user/w205/hospital_compare/hospitals
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare/hospitals

OLD_FILE="Timely and Effective Care - Hospital.csv"
NEW_FILE="effective_care.csv"
tail -n +2 "$OLD_FILE"> $NEW_FILE
# copy the files to hfds
hdfs dfs -mkdir /user/w205/hospital_compare/effective_care
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare/effective_care

OLD_FILE="Readmissions and Deaths - VA.csv"
NEW_FILE="readmissions.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
# copy the files to hfds
hdfs dfs -mkdir /user/w205/hospital_compare/readmissions
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare/readmissions

OLD_FILE="Measure Dates.csv"
NEW_FILE="Measures.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
# copy the files to hfds
hdfs dfs -mkdir /user/w205/hospital_compare/measures
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare/measures

OLD_FILE="hvbp_hcahps_11_10_2016.csv"
NEW_FILE="survey_responses.csv"
tail -n +2 "$OLD_FILE" > $NEW_FILE
# copy the files to hfds
hdfs dfs -mkdir /user/w205/hospital_compare/survey_responses
hdfs dfs -put $NEW_FILE /user/w205/hospital_compare/survey_responses

# change directory back to the original

cd $MY_CWD

# clean exit
exit
