# Dashbase Datasource Plugin

## Dependencies
- [NodeJS][1]
[1]: https://nodejs.org/download/ 

## How To
Download to desired {path} <b>AND</b>
#### Grafana Source Build
Uncomment and set `plugin = {path}` within `src/github.com/grafana/grafana/conf/custom.ini`. Copy from `sample.ini` if this file does not exist.
#### Brew
run `grafana-cli --pluginsDir {path}` 
#### Other Installations
Refer to http://docs.grafana.org/installation/ for more details regarding where to find custom configirations directory.

### Run
```
npm install
grunt
```
Restart Grafana server. 
</br>
The plugin should appear within the Grafana dashboard. If not, ensure the path is set properly via the Grafana logs