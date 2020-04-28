import atexit
import os
from pyspark.sql import SparkSession
from custom_auth.sparkextension import load_extensions

spark = SparkSession.builder.appName(os.environ["JUPYTERHUB_CLIENT_ID"]) \
    .config("spark.submit.deployMode", "client") \
    .config("spark.ssb.dapla.oauth.tokenUrl", os.environ["OAUTH2_TOKEN_URL"]) \
    .config("spark.ssb.dapla.metadata.publisher.url", os.environ["METADATA_PUBLISHER_URL"]) \
    .config("spark.ssb.dapla.data.access.url", os.environ["DATA_ACCESS_URL"]) \
    .config("spark.ssb.dapla.catalog.url", os.environ["CATALOG_URL"]) \
    .getOrCreate()

# This is similar to /pyspark/shell.py
sc = spark.sparkContext
sql = spark.sql
atexit.register(lambda: sc.stop())

# This registers the custom pyspark extensions
load_extensions()
