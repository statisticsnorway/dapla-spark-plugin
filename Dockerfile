FROM apache/zeppelin:0.8.1

# Copy the library.
COPY target/lds-gsim-spark-1.0-SNAPSHOT.jar /lds-gsim-spark.jar

# Libraries are in the interpreter settings
# /zeppelin/conf/interpreter.json
COPY interpreter.json /zeppelin/conf/interpreter.json
