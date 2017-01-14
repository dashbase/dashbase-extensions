import {RapidResponse} from './rapid_response';
import _ from 'lodash';

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
			return new RapidResponse(sentTargets, response).parseGraphResponse(sentTargets);
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
		var queryStr = `SELECT ${target.target} AS "${target.alias}"`;
		var timeRangeFilter = ` BEFORE ${timerange.to.valueOf()} AFTER ${timerange.from.valueOf()}`;
	
		if (!target.query) { // if no query follows the WHERE clause
			queryStr += timeRangeFilter ;
		} else {
			queryStr += ` WHERE ${target.query}` + timeRangeFilter;
		}
		console.log(queryStr);
		return queryStr;
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