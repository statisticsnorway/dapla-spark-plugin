#!/bin/sh
env | grep SPARK | awk '{print "export \"" $0 "\""}' > ${SPARK_HOME}/conf/spark-env.sh
exec $@