'use strict';

System.register(['./datasource', './query_ctrl'], function (_export, _context) {
  "use strict";

  var DashbaseDatasource, DashbaseDatasourceQueryCtrl, DashbaseConfigCtrl;

  function _classCallCheck(instance, Constructor) {
    if (!(instance instanceof Constructor)) {
      throw new TypeError("Cannot call a class as a function");
    }
  }

  return {
    setters: [function (_datasource) {
      DashbaseDatasource = _datasource.DashbaseDatasource;
    }, function (_query_ctrl) {
      DashbaseDatasourceQueryCtrl = _query_ctrl.DashbaseDatasourceQueryCtrl;
    }],
    execute: function () {
      _export('ConfigCtrl', DashbaseConfigCtrl = function DashbaseConfigCtrl() {
        _classCallCheck(this, DashbaseConfigCtrl);
      });

      DashbaseConfigCtrl.templateUrl = 'partials/config.html';

      _export('Datasource', DashbaseDatasource);

      _export('QueryCtrl', DashbaseDatasourceQueryCtrl);

      _export('ConfigCtrl', DashbaseConfigCtrl);
    }
  };
});
//# sourceMappingURL=module.js.map
