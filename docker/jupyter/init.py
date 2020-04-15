import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('jupyter') \
    .config("spark.ssb.access", os.environ["SSB_ACCESS"]) \
    .config("spark.ssb.refresh", os.environ["SSB_REFRESH"]) \
    .getOrCreate()
