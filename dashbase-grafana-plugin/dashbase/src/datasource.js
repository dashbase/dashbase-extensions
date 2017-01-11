import {RapidResponse} from './rapid_response';

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
		// options contains the request object
		var payload = "SELECT ";
		var target;
		var sentTargets = [];

		// currently only supports a single query
		for (var i = 0; i < options.targets.length; i++) {
			target = options.targets[i];
			if (target.hide) {
				continue;
			}
			sentTargets.push(target);
			payload += target.target;
		}
		if (sentTargets.length === 0) {
			return $q.when([]);
		}
		return this._post("sql", payload).then(function(response) {
			return new RapidResponse(sentTargets, response).parseResponse();
		});
	}

	metricFindQuery(options){
		return this._request("GET", "get-info").then(function(response) {
			// fields available from get-info endpoint
		});
	}

	testDatasource() {
		return this._request("GET", "").then(response => {
			if (response.status === 200) {
				return { status: "success", message: "Data source is working.", title: "Success" };
			}
		});
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