# Spark integration with LDS GSIM

This project integrates spark with LDS and the GSIM model. It implements a new format 
so that users can read from and write to a dataset that exists in a LDS instance. 

## Known issues

* No environment settings for the gsim module. Configuration is done via spark configuration. 
See `CONFIG_LDS_OAUTH_TOKEN_URL` in `GsimDatasource.java`. Default config is setup to access 
`https://lds-c.staging.ssbmod.net/ns/` but you'll need to set the `spark.ssb.gsim.oauth.password` 
value in the **spark interpreter** 
settings.
## Test it

The project contains a Dockerfile based on `apache/zeppelin`. It includes the Google Cloud 
Storage connector and the built jar file.

```
# Compile the project
mvn clean package
# Build the image locally
docker build -t lds-zeppelin-gsim .
# Start the service
docker run -p 8080:8080 -v /full/path/to/sa.json:/gcloud/key.json lds-zeppelin-gsim
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

