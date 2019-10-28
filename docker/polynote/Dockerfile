FROM bde2020/spark-base

RUN wget https://github.com/polynote/polynote/releases/download/0.2.8/polynote-dist.tar.gz
RUN tar xzf polynote-dist.tar.gz

COPY config.yml polynote/config.yml
COPY config.yml polynote/config.yml

COPY docker-run-spark-env.sh /
RUN chmod +x docker-run-spark-env.sh

ENV SPARK_HOME=/spark
ENV PATH=${PATH}:${SPARK_HOME}/bin

EXPOSE 8192

CMD ["/docker-run-spark-env.sh", "polynote/polynote"]

