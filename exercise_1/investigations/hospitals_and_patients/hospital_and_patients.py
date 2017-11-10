import sys
sys.path.append('/data/spark15/python/')

import numpy as np
import pandas as pd

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *


sc = SparkContext("local", "hospital_compare")
sqlContext = SQLContext(sc)
hc = HiveContext(sc)

hospitals = hc.sql('select * from hospitals_reduced').toPandas()
effective_care = hc.sql('select * from effective_care_scores').toPandas()
survey_responses = hc.sql('select * from survey_responses_reduced').toPandas()

def hospital_variability():
    df = pd.merge(hospitals, effective_care, how='inner', left_on='provider_id', right_on='provider_id')
    results = df.set_index(['provider_id', 'hospital_name', 'measure_id'])[['score']].unstack()
    results = results.apply(pd.to_numeric)
    results = results.std(axis=1).reset_index().rename(columns={0:'StDev Procedures'})
    return results

def best_hospital_overall():
    df = pd.merge(hospitals, effective_care, how='inner', left_on='provider_id', right_on='provider_id') # merge effective_care_scores and hospitals_reduced
    df['hospital_overall_rating'] = df['hospital_overall_rating'].replace('Not Available', np.nan).apply(pd.to_numeric) # replace string values with numeric and
    return df

def patient_survey_correl():
    variability = hospital_variability()
    hospital_rating = best_hospital_overall()

    survey_responses['overall_rating_of_hospital_performance_rate'] = survey_responses['overall_rating_of_hospital_performance_rate'].replace('Not Available', np.nan).apply(pd.to_numeric)
    survey_responses = survey_responses.rename(columns={'provider_number': 'provider_id'})
    df = pd.merge(survey_responses[['provider_id', 'overall_rating_of_hospital_performance_rate']], hospital_rating[['provider_id', 'hospital_overall_rating']], on='provider_id')
    results = pd.merge(df, variability, on='provider_id')

    print 'correlation between patient survey overall rating and hospital overall rating is '
    print np.round(results['overall_rating_of_hospital_performance_rate'].corr(results['hospital_overall_rating']), 2)

    print '\n'
    print 'correlation between patient survey overall rating and hospital procedure variability is '
    print np.round(results['overall_rating_of_hospital_performance_rate'].corr(results['StDev Procedures']), 2)

    return

if __name__ == '__main__':
    patient_survey_correl()