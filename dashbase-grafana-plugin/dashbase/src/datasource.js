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
		var payload = "";
		var target;
		var sentTargets = []; // keep track of requested list of queries for result matching use

		for (var i = 0; i < options.targets.length; i++) {
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
		return this._post("sql", payload).then(function(response) {
			return new RapidResponseParser(response).parseResponse(sentTargets);
		});
	}

	metricFindQuery(options) {

		// TODO: implement metric listing query (eg. list of available hosts when query is host)
		return this._post("sql", "").then(function(response) {
			var result = []; // temp return empty array
			return result;
		});
	}

	testDatasource() {
		return this._request("GET", "").then(response => {
			if (response.status === 200) {
				return { status: "success", message: "Data source is working.", title: "Success" };
			}
		});
	}

	_buildQueryString(target, timerange) {
		var templateTarget = this.templateSrv.replace(target.target);
		var queryStr = `SELECT ${templateTarget}`;
		if (target.target && this._isAggregation(templateTarget)) {
			queryStr += ` AS "${target.alias}"`;
		} else {
			target.alias = ""; // remove alias as it is not used
		}
		var timeRangeFilter = ` BEFORE ${timerange.to.unix()} AFTER ${timerange.from.unix()}`; // time in seconds

		if (target.query) { // if WHERE query exists
			queryStr += ` WHERE ${this.templateSrv.replace(target.query)}`;
		}
		queryStr += timeRangeFilter;

		if(target.limit) {
			queryStr += ` LIMIT ${this.templateSrv.replace(target.limit)}`;
		}
		return queryStr;
	}

	_isAggregation(str) {
		var aggregations = ["sum(", "min(", "max(", "avg(", "facet(", "histo(", "ts(", "cohort("];
		for (var i = 0; i < aggregations.length; i++) {
			if (_.includes(str, aggregations[i])) return true;
		}
		return false;
	}

	_request(method, endpoint, data) {
		var options = {
			url: this.url + "/" + endpoint,
			method: method,
			data: data
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
}