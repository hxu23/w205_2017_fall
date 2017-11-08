import os, sys
sys.path.append('/data/spark15/python/')
import pandas as pd
import numpy as np

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
import pyspark.sql.functions

sc = SparkContext("local", "hospital_compare")
sqlContext = SQLContext(sc)
hc = HiveContext(sc)

scored_columns = ['communication_with_nurses_dimension_score',
                  'communication_with_doctors_dimension_score',
                  'responsiveness_of_hospital_staff_dimension_score',
                  'communication_about_medicines_dimension_score',
                  'cleanliness_and_quietness_of_hospital_environment_dimension_score',
                  'discharge_information_dimension_score',
                  'overall_rating_of_hospital_dimension_score']

def get_numeric_out_of(series):
    series = series.str.split(' out of ').str.get(0)
    return series

def transform_survey():
    survey_responses = hc.sql('select * from survey_responses').toPandas()
    survey_responses[scored_columns] = survey_responses[scored_columns].apply(get_numeric_out_of)
    return survey_responses

def store_survey():
    hc.sql('DROP TABLE IF EXISTS survey_responses_reduced')
    survey_responses_reduced = transform_survey()
    spark_df = hc.createDataFrame(survey_responses_reduced)
    spark_df.write.saveAsTable('survey_responses_reduced')
    return


if __name__ == '__main__':
    store_survey()
    print 'stored into HIVE survey_responses_reduced'