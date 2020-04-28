import os
import requests
import jwt
import time
from pyspark.sql import DataFrameReader
from pyspark.sql import SparkSession
from pyspark import SparkContext
from jupyterhub.services.auth import HubAuth

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
        # Fetch new access token
        update_tokens()
    return session

def should_reload_token(conf):
    spark_token = conf.get("spark.ssb.access")
    if spark_token is None:
        # First time fetching the token
        return True

    access_token = jwt.decode(spark_token, verify=False)
    diff_access = access_token['exp'] - time.time()
    # Should fetch new token from server if the access token within the given buffer
    if diff_access > int(os.environ['SPARK_USER_TOKEN_EXPIRY_BUFFER_SECS']):
        return False
    else:
        return True

def update_tokens():
    # Helps getting the correct ssl configs
    hub = HubAuth()
    response = requests.get(os.environ['JUPYTERHUB_HANDLER_CUSTOM_AUTH_URL'],
             headers={
                 'Authorization': 'token %s' % hub.api_token
             }, cert = (hub.certfile, hub.keyfile), verify= hub.client_ca).json()
    SparkContext._active_spark_context._conf.set("spark.ssb.access", response['access_token'])

