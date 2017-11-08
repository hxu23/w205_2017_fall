import os, sys
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


def best_hospital_overall():
    df = pd.merge(hospitals, effective_care, how='inner', left_on='provider_id', right_on='provider_id') # merge effective_care_scores and hospitals_reduced
    df['hospital_overall_rating'] = df['hospital_overall_rating'].replace('Not Available').apply(pd.to_numeric) # replace string values with numeric and

    print (df.groupby(['provider_id', 'hospital_name']).mean()[['hospital_overall_rating']]
                                                       .sort_values(by='hospital_overall_rating',
                                                                    ascending=False)).head(10)
    return

def best_hospital_by_measures():
    df = pd.merge(hospitals, effective_care, how='inner', left_on='provider_id', right_on='provider_id')
    results = df.set_index(['provider_id', 'hospital_name', 'measure_id'])[['score']].unstack() # index by hospital and measure_id
    results = results.apply(pd.to_numeric) # convert values to numeric
    results.loc[:, 'Average_Score'] = results.mean(axis=1) # average score across all measures
    results.loc[:, 'StDev_Score'] = results.std(axis=1) # standard deviation of scores across all measures
    print results.sort_values(by='Average_Score', ascending=False).head(10)
    return

if __name__ == '__main__':
    best_hospital_overall()
    best_hospital_by_measures()