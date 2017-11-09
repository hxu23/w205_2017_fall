import sys
sys.path.append('/data/spark15/python/')

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

def best_state():
    hospitals['hospital_overall_rating'] = hospitals['hospital_overall_rating'].replace('Not Available', np.nan).apply(pd.to_numeric)
    print 'Top 10 States by Overall Hospital Rating'
    print hospitals.groupby('state').mean().sort_values(by='hospital_overall_rating', ascending=False).head(10)

    df = pd.merge(hospitals, effective_care, how='inner', left_on='provider_id', right_on='provider_id')
    results = df.set_index(['state', 'provider_id', 'hospital_name', 'measure_id'])[['score']].unstack()
    results = results.apply(pd.to_numeric)
    results.loc[:, 'Average_Score'] = results.mean(axis=1)
    results.loc[:, 'StDev_Score'] = results.std(axis=1)

    print '\nTop 10 States by Procedure Measures Average Score'
    print results.mean(level='state', axis=0).sort_values(by='Average_Score', ascending=False).head(10)['Average_Score']
    return

if __name__ == '__main__':
    best_state()

