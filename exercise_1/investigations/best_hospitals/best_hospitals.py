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


def find_best_hospital_overal():
    df = pd.merge(hospitals, effective_care, how='inner', left_on='provider_id', right_on='provider_id')
    df['hospital_overall_rating'] = df['hospital_overall_rating'].replace('Not Available').apply(pd.to_numeric)

    print (df.groupby(['provider_id', 'hospital_name']).mean()[['hospital_overall_rating']]
                                                       .sort_values(by='hospital_overall_rating', ascending=False)).head(10)
    return

if __name__ == '__main__':
    find_best_hospital_overal()