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
		var query = "";
		var target;
		var sentTargets = [];
		// currently only supports a single query
		for (var i = 0; i < options.targets.length; i++) {
			target = options.targets[i];
			if (target.hide) {
				continue;
			}
			sentTargets.push(target);
			query = target.target;
		}
		if (sentTargets.length === 0) {
			return $q.when([]);
		}
		console.log(angular.toJson(options));
		return this._post("sql", query);
	}

	metricFindQuery(options){
		return this._request("GET", "get-info").then(function(results) {
			// fields available from get=info endpoint
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
		return this._request("POST", endpoint, data).then(function(results) {
			console.log(results.data.aggregations.ts_day.histogramBuckets[0]);
			var histogramBucket = results.data.aggregations.ts_day.histogramBuckets[0];

			var jsonResponse = { data: [
			{
				"target": "select ts(day)",
				"datapoints":[
				[histogramBucket.count, new Date().getTime()]
				]
			}
			]}
			console.log(jsonResponse);
			return jsonResponse;
		});
	}
}