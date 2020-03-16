FROM eu.gcr.io/prod-bip/dapla-zeppelin-base:latest

# Add the GCS connector.
# https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/INSTALL.md#configure-spark
# https://github.com/GoogleCloudPlatform/bigdata-interop/issues/188
# https://github.com/GoogleCloudPlatform/bigdata-interop/pull/180/files
RUN wget -P /tmp https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-2.0.1.jar && \
    mv /tmp/gcs-connector-hadoop2-2.0.1.jar /zeppelin/lib/gcs-connector-hadoop.jar

# Merge the content of the interpreter settings.
COPY docker/zeppelin/interpreter-template.json /zeppelin/conf/interpreter-new.json

USER root
RUN apk --no-cache add --virtual jq-dependencies jq && \
    mv /zeppelin/conf/interpreter.json /zeppelin/conf/interpreter-original.json && \
    jq -n 'reduce inputs as $i ({}; . * $i)' /zeppelin/conf/interpreter-original.json \
                                             /zeppelin/conf/interpreter-new.json > \
                                             /zeppelin/conf/interpreter-template.json && \
    apk del jq-dependencies

COPY docker/zeppelin/env.sh /zeppelin/bin/env.sh
RUN apk --no-cache add gettext

# Copy the library.
COPY target/dapla-spark-plugin-*-shaded.jar /zeppelin/lib/dapla-spark-plugin.jar

# Install the markdown interpreter.
RUN ZEPPELIN_INTERPRETER_DEP_MVNREPO=https://repo1.maven.org/maven2 \
    /zeppelin/bin/install-interpreter.sh --name md --artifact org.apache.zeppelin:zeppelin-markdown:0.8.2

RUN chown -R zeppelin:zeppelin /zeppelin
USER zeppelin:zeppelin
CMD ["/zeppelin/bin/env.sh"]
