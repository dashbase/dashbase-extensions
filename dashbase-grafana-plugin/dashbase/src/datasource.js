import {RapidResponseParser} from './rapid_response_parser';

export class DashbaseDatasource {

  constructor(instanceSettings, $q, backendSrv, templateSrv, timeSrv) {
    this.basicAuth = instanceSettings.basicAuth;
    this.withCredentials = instanceSettings.withCredentials;
    this.type = instanceSettings.type;
    this.url = instanceSettings.url;
    this.name = instanceSettings.name;
    this.q = $q;
    this.backendSrv = backendSrv;
    this.templateSrv = templateSrv;
    this.timeSrv = timeSrv;
  }

  query(options) {
    // options contains the request object, targets being the list of queries on the graph
    let payload = "";
    let target;
    let sentTargets = []; // keep track of requested list of queries for result matching use

    for (let i = 0; i < options.targets.length; i++) {
      target = options.targets[0]; // currently only supports the first target
      if (target.hide) {
        continue;
      }
      if (!target.alias) { // if no alias is provided
        target.alias = target.target;  // use sql syntax as alias
      } // otherwise use user provided alias

      sentTargets.push(target);
      payload = this._buildQueryString(target, options.range);
    }
    if (sentTargets.length === 0) {
      return $q.when([]);
    }
    return this._get("v1/sql", payload).then(function (response) {
      return new RapidResponseParser(response).parseResponse(sentTargets);
    });
  }

  metricFindQuery(options) {

    // TODO: implement metric listing query (eg. list of available hosts when query is host)
    return this._post("sql", "").then(function (response) {
      let result = []; // temp return empty array
      return result;
    });
  }

  testDatasource() {
    return this._request("GET", "v1/sql").then(response => {
      if (response.status === 200) {
        return {
          status: "success",
          message: "Data source is connected.",
          title: "Success"
        };
      }
    });
  }

  _buildQueryString(target, timerange) {
    let templateTarget = this.templateSrv.replace(target.target);
    if (templateTarget == "*") templateTarget = "\\*";
    let queryStr = `SELECT%20${templateTarget}`;
    if (target.target && this._isAggregation(templateTarget)) {
      queryStr += `%20AS%20"${target.alias}"`;
    } else {
      target.alias = ""; // remove alias as it is not used
    }
    let timeRangeFilter = ` BEFORE%20${timerange.to.unix()}%20AFTER%20${timerange.from.unix()}`; // time in seconds

    if (target.query) { // if WHERE query exists
      queryStr += `%20WHERE%20${this.templateSrv.replace(target.query)}`;
    }
    queryStr += timeRangeFilter;

    if (target.limit) {
      queryStr += `%20LIMIT%20${this.templateSrv.replace(target.limit)}`;
    }
    return queryStr;
  }

  _isAggregation(str) {
    let aggregations = [
      "sum(", "min(", "max(", "avg(", "facet(", "histo(",
      "ts(", "cohort("
    ];
    for (let i = 0; i < aggregations.length; i++) {
      if (_.includes(str, aggregations[i])) {
        return true;
      }
    }
    return false;
  }

  _request(method, endpoint, data) {
    let options;
    let endpointAsUrl = this.url + "/" + endpoint;
    if (method == "GET") {
      endpointAsUrl += "\?sql=" + data;
      options = {
        url: endpointAsUrl,
        method: method,
      }
    } else {
      options = {
        url: endpointAsUrl,
        method: method,
        data: data
      }
    }

    if (this.basicAuth || this.withCredentials) {
      options.withCredentials = true;
    }
    if (this.basicAuth) {
      options.headers = {
        "Authorization": this.basicAuth
      };
    }
    return this.backendSrv.datasourceRequest(options);
  }

  _post(endpoint, data) {
    return this._request("POST", endpoint, data);
  }

  _get(endpoint, data) {
    return this._request("GET", endpoint, data);
  }
}