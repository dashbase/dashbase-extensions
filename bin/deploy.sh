#!/bin/bash

#########################
### DEPLOYMENT SCRIPT ###
#########################
VERSION=0.0.1

# Upload Grafana tarball
aws s3 cp dashbase-grafana/dashbase/target/dashbase-grafana-datasource-$VERSION.tar.gz s3://dashbase-builds/master/dashbase-grafana-datasource-$VERSION.tar.gz

# Update Grafana server
ssh ec2-user@staging.dashbase.io "./update_grafana.sh"
