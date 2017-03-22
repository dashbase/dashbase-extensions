#!/bin/bash

#########################
### DEPLOYMENT SCRIPT ###
#########################
VERSION=0.0.1

configure_aws_cli() {
  aws --version
  aws configure set default.region us-west-1
}

upload_to_s3() {
  # Upload Grafana tarball
  aws s3 cp dashbase-grafana/target/dashbase-grafana-datasource-$VERSION.tar.gz s3://dashbase-builds/master/dashbase-grafana-datasource-$VERSION.tar.gz

  # Update Grafana server
  ssh ec2-user@staging.dashbase.io "./update_grafana.sh"

}

# Configure AWS
configure_aws_cli

# Push latest to S3
upload_to_s3
