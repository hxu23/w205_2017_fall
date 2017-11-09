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

def hospital_variability():
    df = pd.merge(hospitals, effective_care, how='inner', left_on='provider_id', right_on='provider_id')
    results = df.set_index(['provider_id', 'hospital_name', 'measure_id'])[['score']].unstack()
    results = results.apply(pd.to_numeric)
    print 'Top 10 procedures with Highest Variability Across Hospitals - Measure IDs'
    print results.std(axis=0).sort_values(ascending=False).head(10)
    return

if __name__ == '__main__':
    hospital_variability()