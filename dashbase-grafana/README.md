# Dashbase Datasource Plugin

## Dependencies
- [NodeJS][1]
[1]: https://nodejs.org/download/ 

## How To
Download to desired path and set custom plugin path in Grafana configuration.
</br>
#### Grafana Source Build
Uncomment and set `plugin = /path/to/dashbase-extensions/dashbase-grafana-plugin/` within `src/github.com/grafana/grafana/conf/custom.ini`. Copy from `sample.ini` if this file does not exist.
</br>
#### Brew
Uncomment and set `plugin = /path/to/dashbase-extensions/dashbase-grafana-plugin/` within `/usr/local/etc/grafana/grafana.ini`. 
#### Other Installations
Refer to http://docs.grafana.org/installation/ for more details regarding where to find custom configurations directory.
</br>
## Build
```
cd /path/to/dashbase-extensions/dashbase-grafana-plugin/dashbase
* Note: May need to install grunt globally; if so, npm install -g grunt
npm install
grunt
```
</br>
Restart Grafana server. 
</br>
The plugin should appear within the Grafana dashboard. If not, ensure the path is set properly via the Grafana logs
