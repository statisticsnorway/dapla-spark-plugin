FROM apache/zeppelin:0.8.1


# Copy the library.
COPY target/lds-gsim-spark-1.0-SNAPSHOT.jar /zeppelin/lib/lds-gsim-spark.jar

# Copy zeppelin settings. This includes the library.
# docker exec ID-OF-CONTAINER cat /zeppelin/conf/interpreter.json > interpreter.json
COPY interpreter-template.json /zeppelin/conf/interpreter-template.json
COPY zeppelin-site.xml /zeppelin/conf/zeppelin-site.xml
COPY env.sh /env.sh

# Add the GCS connector.
# https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/INSTALL.md#configure-spark
# https://github.com/GoogleCloudPlatform/bigdata-interop/issues/188
# https://github.com/GoogleCloudPlatform/bigdata-interop/pull/180/files
RUN wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar
RUN mv gcs-connector-hadoop2-latest.jar lib/gcs-connector-hadoop.jar

# Add standalone spark
# Using same version as dataproc
# Spark 2.4.3 (git revision f60fb14) built for Hadoop 2.9.2
# (Build flags: -B -e -Dhadoop.version=2.9.2 -Dyarn.version=2.9.2 -Dzookeeper.version=3.4.13
#   -Dprotobuf.version=2.5.0 -Dscala.version=2.11.12 -Dscala.binary.version=2.11 -Pscala-2.11 -Pflume -Phive -Phive-thriftserver -Pkubernetes)
RUN wget http://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz \
&&  tar -xzf spark-2.4.3-bin-hadoop2.7.tgz \
&&  mv spark-2.4.3-bin-hadoop2.7 /opt/spark

# Fix for https://issues.apache.org/jira/browse/ZEPPELIN-3939
RUN wget http://repo1.maven.org/maven2/org/apache/commons/commons-lang3/3.5/commons-lang3-3.5.jar \
&&  cp commons-lang3-3.5.jar /zeppelin/lib \
&&  cp commons-lang3-3.5.jar /zeppelin/lib/interpreter \
&&  cp commons-lang3-3.5.jar /zeppelin/interpreter/cassandra \
&&  cp commons-lang3-3.5.jar /zeppelin/interpreter/sh \
&&  cp commons-lang3-3.5.jar /zeppelin/interpreter/scio \
&&  cp commons-lang3-3.5.jar /zeppelin/interpreter/flink \
&&  cp commons-lang3-3.5.jar /zeppelin/interpreter/alluxio \
&&  cp commons-lang3-3.5.jar /zeppelin/interpreter/md \
&&  cp commons-lang3-3.5.jar /zeppelin/interpreter/elasticsearch \
&&  cp commons-lang3-3.5.jar /zeppelin/interpreter/pig \
&&  cp commons-lang3-3.5.jar /zeppelin/interpreter/sap \
&&  rm /zeppelin/lib/commons-lang3-3.4.jar \
&&  rm /zeppelin/interpreter/sh/commons-lang3-3.4.jar \
&&  rm /zeppelin/interpreter/scio/commons-lang3-3.4.jar \
&&  rm /zeppelin/interpreter/flink/commons-lang3-3.4.jar \
&&  rm /zeppelin/interpreter/alluxio/commons-lang3-3.4.jar \
&&  rm /zeppelin/interpreter/md/commons-lang3-3.4.jar \
&&  rm /zeppelin/interpreter/elasticsearch/commons-lang3-3.4.jar \
&&  rm /zeppelin/interpreter/pig/commons-lang3-3.4.jar \
&&  rm /zeppelin/interpreter/sap/commons-lang3-3.4.jar \
&&  rm /zeppelin/interpreter/cassandra/commons-lang3-3.3.2.jar \
&&  rm /zeppelin/lib/interpreter/commons-lang3-3.4.jar

# Also, the old commons-lang3 library is bundled in spark-interpreter jar file. Replace this with a custom one that has
# been updated
RUN rm /zeppelin/interpreter/spark/spark-interpreter-0.8.1.jar
COPY spark-interpreter-0.8.2-SNAPSHOT.jar /zeppelin/interpreter/spark/spark-interpreter-0.8.2-SNAPSHOT.jar

RUN echo "$LOG_TAG Update java8 version" && \
    apt-get -y update && \
    apt-get install -y openjdk-8-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark

VOLUME ["/gcloud/key.json"]

# Add some examples:
# val test = spark.read.parquet("gs://ssb-data-a/data/b9c10b86-5867-4270-b56e-ee7439fe381e")
# test.show()
#
# val test = spark.read.format("no.ssb.gsim.spark")
#     .load("lds+gsim://b9c10b86-5867-4270-b56e-ee7439fe381e")
# test.show()

WORKDIR /zeppelin
CMD ["/env.sh"]
