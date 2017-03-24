#!/bin/bash

VERSION=0.0.1

sudo aws s3 cp s3://dashbase-builds/master/dashbase-grafana-app-$VERSION.tar.gz /var/lib/grafana/plugins/dashbase-grafana.tar.gz

sudo tar -xf /var/lib/grafana/plugins/dashbase-grafana.tar.gz -C /var/lib/grafana/plugins/

sudo rm /var/lib/grafana/plugins/dashbase-grafana.tar.gz

# Restart Grafana server
sudo service grafana-server restart
