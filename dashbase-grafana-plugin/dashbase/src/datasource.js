export class DashbaseDatasource {

	constructor(instanceSettings, $q, backendSrv, templateSrv) {
		this.type = instanceSettings.type;
		this.url = instanceSettings.url;
		this.name = instanceSettings.name;
		this.q = $q;
		this.backendSrv = backendSrv;
		this.templateSrv = templateSrv;
	}

	testDatasource() {
		return this.backendSrv.datasourceRequest({
			url: this.url + '/',
			method: 'GET'
		}).then(response => {
			if (response.status === 200) {
				return { status: "success", message: "Data source is working.", title: "Success" };
			}
		});
	}
}
