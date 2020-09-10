# Dapla Spark Plugin 

[![Build Status](https://dev.azure.com/statisticsnorway/Dapla/_apis/build/status/statisticsnorway.dapla-spark-plugin?repoName=statisticsnorway%2Fdapla-spark-plugin&branchName=master)](https://dev.azure.com/statisticsnorway/Dapla/_build/latest?definitionId=99&repoName=statisticsnorway%2Fdapla-spark-plugin&branchName=master)

This project integrates spark with dapla services and GCS.

### Usage

The integration is implemented as a custom `gsim` (or `no.ssb.dapla.spark.plugin`) format. You can read and write datasets 

For testing locally. Setup [localstack](https://github.com/statisticsnorway/dapla-project/blob/master/localstack/README.md)

### Junit tests

In order to run some of the junit tests for the full stack you will need to have a service account file available in your environment.
Ask a friend to get hold of this.

  - The project _dapla-spark-plugin_ needs a service account key file placed under`/secret/gcs_sa_test.json`.
This is needed to build the docker image and to run integration tests against a GCS bucket.

#### Redeployment locally:
build plugin `mvn clean install` and copy to zeppelin docker container<br>
`docker cp target/*-shaded.jar zeppelin-notebook:/zeppelin/lib/dapla-spark-plugin.jar`<br>
Or use `make spark-plugin-redeploy` in [dapla-project/localstack](https://github.com/statisticsnorway/dapla-project)    

#### Deployment to staging - Alternative 1:
Copy target/dapla-spark-plugin-0.3.0-SNAPSHOT-shaded.jar to https://github.com/statisticsnorway/zeppelin-docker/tree/master/files/zeppelin/dapla-spark-plugin.jar
Push changes and a drone will build a zeppelin docker instance and this will then be deployed to https://zeppelin.staging-bip-app.ssb.no/

#### Deployment to staging - Alternative 2: 
Release plugin to nexus with drone. Then use `wget` from zepplin to get plugin
Example: wget https://nexus.prod-bip-ci.ssb.no/repository/maven-snapshots/no/ssb/dapla/spark/plugin/dapla-spark-plugin/0.3.0.RUNE-SNAPSHOT/dapla-spark-plugin-0.3.0.RUNE-20200124.122111-2-shaded.jar -O /zeppelin/dapla-spark-plugin.jar
Then reset the spark interpreter

#### populate test data
Before you can read/write a dataset you need to have a user with access registered. [Doc](https://github.com/statisticsnorway/dapla-project)
Use [run-scenario.sh](https://github.com/statisticsnorway/dapla-project/blob/master/localstack/bin/run-scenario.sh)<br>
Example for populating locally:      
in folder `dapla-project/localstack/bin` run `./run-scenario.sh exec local demo1`<br>
Or use `make run-scenario` from `dapla-project/localstack`

#### Login to zeppelin locally 
Open http://localhost:28010/ (check port for zeppelin [her](https://github.com/statisticsnorway/dapla-project/blob/master/localstack/docker-compose.yml) )<br>
Log into using user:`user1` and password:`password2`<br> 
Or create your own user [here](https://github.com/statisticsnorway/dapla-project/blob/master/localstack/docker/zeppelin/shiro.ini)   
 
### Usage in spark
```scala
val dataset = Seq(
  ("John Doe", "male", "0101")
).toDF("person_id", "gender", "MUNICIPALITY")

// Write
dataset.write
    .format("gsim")
    .option("valuation", "INTERNAL")
    .option("state", "INPUT")
    .mode("overwrite")
    .save("skatt.person/testfolder/testdataset")

// Read 
val dataset = spark.read.format("gsim")
    .load("skatt.person/testfolder/testdataset")
```
For getting sample parquet files from staging
use gsutil and copy from gs://ssb-data-staging to docker-compose shared volume
`gsutil cp gs://ssb-data-staging/datastore/skatt/person/rawdata-2019/skatt-2-levels-v0.53.parquet dapla-project/localstack/data/rawdata-2019`<br>
Update [demo1.sh](https://github.com/statisticsnorway/dapla-project/blob/master/localstack/bin/scenarios/demo1.sh) with update location to the data copied in.
```
## create dataset
put $spark '/dataset-meta?userId=user1' '{
  "id": {
    "id": "341b03d6-5be6-4c9b-b381-8cf692aa8830",
    "name": ["skatt.person.2019.rawdata"]
  },
  "valuation": "SHIELDED",
  "state": "RAW",
  "locations": ["file:///datastore/skatt/person/rawdata-2019"]
}' 200

```
Then we can read this
```scala
// Read 
val skattRawdata = spark.read.format("gsim")
    .load("skatt.person.2019.rawdata")

```
