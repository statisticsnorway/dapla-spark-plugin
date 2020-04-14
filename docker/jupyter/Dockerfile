FROM openjdk:8-alpine

# Intall spark.
ENV SPARK_HOME "/usr/lib/spark"
RUN wget https://apache.uib.no/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz && \
    tar -zxpf spark-2.4.5-bin-hadoop2.7.tgz -C /tmp && \
    mv /tmp/spark-2.4.5-bin-hadoop2.7 ${SPARK_HOME} && \
    rm -rf /tmp/spark-2.4.5-bin-hadoop2.7 && \
    rm spark-2.4.5-bin-hadoop2.7.tgz
ENV PATH="$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin"

RUN echo "**** install Python ****" && \
    apk add --no-cache python3 linux-pam && \
    if [ ! -e /usr/bin/python ]; then ln -sf python3 /usr/bin/python ; fi

RUN echo "**** install pip ****" && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --no-cache --upgrade pip setuptools wheel && \
    if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi

RUN echo "**** install npm ****" && \
    apk add --no-cache npm


RUN echo "**** install jupyterhub ****" && \
    apk add --virtual build-dependencies build-base python3-dev libffi-dev openssl-dev && \
    python3 -m pip install jupyterhub && \
    npm config set unsafe-perm true && \
    npm install -g configurable-http-proxy && \
    python3 -m pip install notebook && \
    apk del build-dependencies

RUN python3 -m pip install jupyterlab
RUN jupyter kernelspec uninstall python3 -f

RUN python3 -m pip install oauthenticator pyjwt

RUN python3 -m pip install toree && \
    jupyter toree install --spark_home=${SPARK_HOME} && \
    apk add --no-cache bash

RUN apk add --update openssl && \
    rm -rf /var/cache/apk/*

ENV JUPYTER_GROUP_ID=2100
ENV JUPYTER_USER_ID=2100
ENV JUPYTER_HOME="/jupyter"

# Copy the library.
COPY target/dapla-spark-plugin-*-shaded.jar /jupyter/lib/dapla-spark-plugin.jar

# Add the GCS connector.
# https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/INSTALL.md#configure-spark
# https://github.com/GoogleCloudPlatform/bigdata-interop/issues/188
# https://github.com/GoogleCloudPlatform/bigdata-interop/pull/180/files
RUN wget -P /tmp https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-2.0.1.jar && \
    mv /tmp/gcs-connector-hadoop2-2.0.1.jar /jupyter/lib/gcs-connector-hadoop.jar

# JUPYTER needs a real user/group
RUN addgroup -g $JUPYTER_GROUP_ID -S jupyter && \
    adduser  -u $JUPYTER_USER_ID --shell /bin/false --home ${JUPYTER_HOME} -D -S jupyter -G jupyter

COPY docker/jupyter/env.sh /jupyter/env.sh
RUN chmod +x /jupyter/env.sh

COPY docker/jupyter/custom_auth /usr/lib/python3.6/site-packages/custom_auth

USER jupyter:jupyter

WORKDIR ${JUPYTER_HOME}

CMD ["/jupyter/env.sh"]
