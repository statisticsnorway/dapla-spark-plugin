import atexit
import os
from pyspark.sql import SparkSession
from custom_auth.sparkextension import load_extensions

spark = SparkSession.builder.appName(os.environ["JUPYTERHUB_CLIENT_ID"]).getOrCreate()

# This is similar to /pyspark/shell.py
sc = spark.sparkContext
sql = spark.sql
atexit.register(lambda: sc.stop())

# This registers the custom pyspark extensions
load_extensions()
