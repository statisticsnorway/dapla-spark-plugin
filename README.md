# Spark integration with LDS GSIM

This project integrates spark with LDS and the GSIM model. It implements a new format so that users can read from and write to a dataset that exists in a LDS instance.

## Test it

The project contains a Dockerfile based on `apache/zeppelin`. It includes the Google Cloud Storage connector and the built jar file.

The following environment variables are required.
*Note that if one of the oAuth variable is missing, the module with try to access the lds resources __without__
authentication*

|ENV Variable| Spark variable|Purpose|
|---|---|---|
|LDS_GSIM_SPARK_LOCATION|spark.ssb.gsim.location|Prefix used when writing data|
|LDS_GSIM_SPARK_LDS_URL|spark.ssb.gsim.ldsUrl|LDS url to use|
|LDS_GSIM_SPARK_OAUTH_TOKEN_URL|spark.ssb.gsim.oauth.tokenUrl|OAUTH token url|
|LDS_GSIM_SPARK_OAUTH_CLIENT_ID|spark.ssb.gsim.oauth.clientId|OAUTH client id|
|LDS_GSIM_SPARK_OAUTH_USERNAME|spark.ssb.gsim.oauth.userName|OAUTH username|
|LDS_GSIM_SPARK_OAUTH_PASSWORD|spark.ssb.gsim.oauth.password|OAUTH password|

```bash
# Compile the project
mvn clean package
# Build the image locally
docker build -t lds-zeppelin-gsim .
# Start the service
docker run -p 8080:8080 -e LDS_GSIM_SPARK_LDS_URL=https://lds-c.staging.ssbmod.net/ns/ \
                        -e LDS_GSIM_SPARK_LOCATION=gs://bucker/prefix/ \
                        -e LDS_GSIM_SPARK_OAUTH_TOKEN_URL=https://keycloak.staging.ssbmod.net/auth/realms/ssb/protocol/openid-connect/token \
                        -e LDS_GSIM_SPARK_OAUTH_CLIENT_ID=lds-c-postgres-gsim \
                        -e LDS_GSIM_SPARK_OAUTH_USERNAME=api-user-3 \
                        -e LDS_GSIM_SPARK_OAUTH_PASSWORD=PASSWORD \
                        -v /full/path/to/sa.json:/gcloud/key.json lds-zeppelin-gsim
```

Open the zeppelin [interface](http://localhost:8080/).

Check that all the configuration starting with `spark.ssb.gsim` are setup correctly.

## Usage in spark

Read a dataset:  

```scala
val personWithIncome = spark.read.format("no.ssb.gsim.spark")
    .load("lds+gsim://b9c10b86-5867-4270-b56e-ee7439fe381e")

personWithIncome.printSchema()

personWithIncome.show()
```

Write to a dataset. Note the mode.  

```scala
val myProcessedDataset = Seq(
  ("hadrien", 42, "1", null, "0101", "Very good")
).toDF("PERSON_ID", "INCOME", "GENDER", "MARITAL_STATUS", "MUNICIPALITY", "DATA_QUALITY")

myProcessedDataset.write
  .format("no.ssb.gsim.spark")
  .mode("append")
  .save("lds+gsim://b9c10b86-5867-4270-b56e-ee7439fe381e");
```

## Diagrams

![Append to dataset](http://www.plantuml.com/plantuml/png/VL7DJiCm3BxxAQpSzm8x84qhJ4XmYYQatRBKgyAof4fSnkFZqB51ct6Bldn_jjcn9rprFOKMEM9hs6HY06Cv9jncIj2RnCwwtWH6jIFXUXVKN_YbqNC4D_hv5RN0HmKsUa-MNGmPrJU6rW-PAIaegNl9HRM9iPD2Qn-75hLKC1qfWD835m_uauvBVFmaEp315PBlYQ-mD4idV8_xCf3xC4opyAcewhEEfxwarSYJIONTaAUkP9sJ4z4jUhgKcLRy2hJ4LJxYGIxWNUQRWVn18XvIg4ehLIx5CT1vzBeV-LRA_aySxpmsm2VdAPWJGNKhrKjUKmwY_RMN-jalEKqCTE_z1G00)

![Docker deployment diagram](http://www.plantuml.com/plantuml/png/LP0nRmCX38Lt_mgBhOEb6wEkZPGE7IerMzN11OUWS6S4P6Yh_lV2eLEQ0Ga_VtuFKqEDWdkr5yde94NzccMfw0Bxp3E0ZNfrQ0wgle5FQrMgPlPYaCjs1xRjWjSY6HnN_kGYQ5xsRtWeOLx9w0e0d9ghuBUa934ir4Jy0KIhSzAb9s-jEx4apfUcSAxXrABmlGsIRzQqjZvwG2_l66yBMLqMwM-ZCplLD4XRu1TW7KMQ7kYMEZGb6dR_oZRJpi2thJip5FDyFBwQiMJ_1QGS_BdUdF4HEuAxQJVz0G00)

## Setup in DataProc

### Create DataProc cluster

To create the lds spark interface in DataProc use this (or similar) command:

  gcloud dataproc clusters create lds-spark-gsim \
    --enable-component-gateway \
    --region europe-north1 \
    --subnet default \
    --zone "" \
    --master-machine-type n1-highmem-4 \
    --master-boot-disk-size 500 \
    --num-workers 2 \
    --worker-machine-type n1-highmem-4 \
    --worker-boot-disk-size 500 \
    --image-version 1.4-debian9 \
    --optional-components ZEPPELIN \
    --project bip-nullfem \
    --properties "dataproc:dataproc.conscrypt.provider.enable=false" \
    --initialization-actions gs://dataproc-initialization-actions/python/pip-install.sh

The setting "enable-component-gateway" will make the Zeppelin web interface available from GCP console, but is not supported yet in Terraform, so the cluster needs to be created manually.
When <https://github.com/terraform-providers/terraform-provider-google/pull/4073> is merged, Zeppelin can be added as a "optional component". The branch <https://github.com/statisticsnorway/platform/tree/dataproc_lds_terraform_example> include a WIP example of the setup.

The property dataproc:dataproc.conscrypt.provider.enable=false is used to disable Conscrypt Java security provider to support lds-gsim-spark interpeter. With Conscrypt it will currently result in a bug in DataProc.

### Manually copy the lds-gsim-spark to the DataProc master node

The jar needs to be manually added to the master node.
Get the SSH and SCP command from the console, under VM Instances.

Example, build the jar and copy it to the master node:

  mvn clean package
  gcloud compute scp --project "bip-nullfem" --zone "europe-north1-c" \
    target/lds-gsim-spark-1.0-SNAPSHOT.jar "lds-spark-gsim-m:"

Connect to the master VM Instance:

  gcloud compute --project "bip-nullfem" ssh --zone "europe-north1-c" "lds-spark-gsim-m"

Move the file to Zeppelin folder and change permissions:

  sudo cp lds-gsim-spark-1.0-SNAPSHOT.jar /etc/zeppelin/lds-gsim-spark.jar
  sudo chown zeppelin /etc/zeppelin/lds-gsim-spark.jar
  sudo chgrp zeppelin /etc/zeppelin/lds-gsim-spark.jar

### Add configuration to the Zeppelin interface

1. Go to Web Interfaces under DataProc Clusters in the GCP Console, click "Zeppelin" under "Component gateway". This will open up the Zeppelin interface.

2. Click the user menu at the right and select "Interpeter". Under "spark" interpeter, click "edit".

3. Under Dependencies, add "/etc/zeppelin/lds-gsim-spark.jar" as artifact.

4. Under "Properties" and name, add the settings, with string values:

|Spark variable|Example|
|---|---|
|spark.ssb.gsim.location|gs://ssb-data-a/data/|
|spark.ssb.gsim.ldsUrl|<https://lds-c.staging.ssbmod.net/ns/>|
|spark.ssb.gsim.oauth.tokenUrl|<https://keycloak.staging.ssbmod.net/auth/realms/ssb/protocol/openid-connect/token>|
|spark.ssb.gsim.oauth.clientId|lds-c-postgres-gsim|
|spark.ssb.gsim.oauth.userName|api-user-3|
|spark.ssb.gsim.oauth.password|Get the password from Keycloak|

Click Save and  OK for the dialog to restart the interpeter.
