import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(os.environ["JUPYTERHUB_CLIENT_ID"]) \
    .config("spark.submit.deployMode", "client") \
    .config("spark.ssb.access", os.environ["SSB_ACCESS"]) \
    .config("spark.ssb.refresh", os.environ["SSB_REFRESH"]) \
    .config("spark.ssb.dapla.oauth.tokenUrl", os.environ["OAUTH2_TOKEN_URL"]) \
    .config("spark.ssb.dapla.metadata.publisher.url", os.environ["METADATA_PUBLISHER_URL"]) \
    .config("spark.ssb.dapla.data.access.url", os.environ["DATA_ACCESS_URL"]) \
    .config("spark.ssb.dapla.catalog.url", os.environ["CATALOG_URL"]) \
    .getOrCreate()
