#!/bin/bash

VERSION=0.0.1

# if exists, mv the grafana tarball to plugins dir
sudo mv /home/ec2-user/dashbase-grafana.tar.gz /var/lib/grafana/plugins/

sudo tar -xf /var/lib/grafana/plugins/dashbase-grafana.tar.gz -C /var/lib/grafana/plugins/

sudo rm /var/lib/grafana/plugins/dashbase-grafana.tar.gz

# Restart Grafana server
sudo service grafana-server restart
