#!/bin/bash

#########################
### DEPLOYMENT SCRIPT ###
#########################
VERSION=0.0.1-SNAPSHOT

# Upload Zeppelin jar
aws s3 cp dashbase-zeppelin/target/dashbase-zeppelin-$VERSION.jar s3://dashbase-builds/master/dashbase-zeppelin/target/dashbase-zeppelin-$VERSION.jar

# Package Grafana dist directory
tar -czf dashbase-grafana-$VERSION-release.tar.gz dashbase-grafana/dist

aws s3 cp dashbase-grafana
