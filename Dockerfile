FROM apache/zeppelin:0.8.1

# Copy the library.
COPY target/lds-gsim-spark-1.0-SNAPSHOT.jar /lds-gsim-spark.jar

# Copy zeppelin settings. This includes the library.
# docker exec ID-OF-CONTAINER cat /zeppelin/conf/interpreter.json > interpreter.json
COPY interpreter.json /zeppelin/conf/interpreter.json

# Add some examples:
# val test = spark.read.format("no.ssb.gsim.spark")
#                .load("lds+gsim://lds-postgres.staging.ssbmod.net/UnitDataset/b9c10b86-5867-4270-b56e-ee7439fe381e");
# test.show();