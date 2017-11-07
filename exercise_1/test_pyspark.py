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

if __name__ == '__main__':
    print(hc.tableNames())