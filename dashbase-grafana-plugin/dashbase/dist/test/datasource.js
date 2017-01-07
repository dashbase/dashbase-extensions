'use strict';

Object.defineProperty(exports, "__esModule", {
	value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var DashbaseDatasource = exports.DashbaseDatasource = function () {
	function DashbaseDatasource(instanceSettings, $q, backendSrv, templateSrv) {
		_classCallCheck(this, DashbaseDatasource);

		this.type = instanceSettings.type;
		this.url = instanceSettings.url;
		this.name = instanceSettings.name;
		this.q = $q;
		this.backendSrv = backendSrv;
		this.templateSrv = templateSrv;
	}

	_createClass(DashbaseDatasource, [{
		key: 'testDatasource',
		value: function testDatasource() {
			return this.backendSrv.datasourceRequest({
				url: this.url + '/',
				method: 'GET'
			}).then(function (response) {
				if (response.status === 200) {
					return { status: "success", message: "Data source is working.", title: "Success" };
				}
			});
		}
	}]);

	return DashbaseDatasource;
}();
//# sourceMappingURL=datasource.js.map
