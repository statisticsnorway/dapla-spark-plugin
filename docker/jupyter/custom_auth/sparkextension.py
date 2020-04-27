import os
import requests
import jwt
import time
from pyspark.sql import DataFrameReader
from pyspark.sql import SparkSession
from pyspark import SparkContext

"""
This extension will replace `spark.read.format("gsim").load("/ns")` with
 `spark.read.namespace("/ns")`, and at the same time add access token reload the spark context if necessary.
"""
def load_extensions():
    DataFrameReader.namespace = namespace_read

def namespace_read(self, ns):
    return get_session().read.format("gsim").load(ns)

def get_session():
    session = SparkSession._instantiatedSession
    if should_reload_token(session.sparkContext.getConf()):
        # Load new spark session
        print("Fetch new access token")
        update_tokens()
    else:
        print("Reusing access token")
    return session

def should_reload_token(conf):
    spark_token = conf.get("spark.ssb.access")
    if spark_token is None:
        print("No spark token")
        return True

    access_token = jwt.decode(spark_token, verify=False)
    diff_access = access_token['exp'] - time.time()
    # Should fetch new token from server if the access token expires in 10 secs
    if diff_access>10:
        return False
    else:
        return True

def update_tokens():
    response = requests.get("http://127.0.0.1:8081/hub/custom-api/user",
                            headers={
                                'Authorization': 'token %s' % os.environ["JUPYTERHUB_API_TOKEN"]
                            }).json()
    SparkContext._active_spark_context._conf.set("spark.ssb.access", response['access_token'])

