#!/bin/bash

echo "Replacing environment variables"
envsubst < /zeppelin/conf/interpreter-template.json > /zeppelin/conf/interpreter.json

/zeppelin/bin/zeppelin.sh