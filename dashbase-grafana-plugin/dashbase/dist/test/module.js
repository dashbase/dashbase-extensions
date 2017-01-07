'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ConfigCtrl = exports.QueryCtrl = exports.Datasource = undefined;

var _datasource = require('./datasource');

var _query_ctrl = require('./query_ctrl');

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var DashbaseConfigCtrl = function DashbaseConfigCtrl() {
  _classCallCheck(this, DashbaseConfigCtrl);
};

DashbaseConfigCtrl.templateUrl = 'partials/config.html';

exports.Datasource = _datasource.DashbaseDatasource;
exports.QueryCtrl = _query_ctrl.DashbaseDatasourceQueryCtrl;
exports.ConfigCtrl = DashbaseConfigCtrl;
//# sourceMappingURL=module.js.map
