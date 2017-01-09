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
		console.log(options);
	}

	testDatasource() {
		return this._request("GET", "").then(response => {
			if (response.status === 200) {
				return { status: "success", message: "Data source is working.", title: "Success" };
			}
		});
	}

	_request(method, endpoint) {
		var options = {
			url: this.url + "/" + endpoint,
			method: method,
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
}