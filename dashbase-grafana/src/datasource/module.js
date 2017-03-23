import {DashbaseDatasource} from './datasource';
import {DashbaseDatasourceQueryCtrl} from './query_ctrl';

class DashbaseConfigCtrl {}
DashbaseConfigCtrl.templateUrl = 'partials/config.html';

export {
  DashbaseDatasource as Datasource,
  DashbaseDatasourceQueryCtrl as QueryCtrl,
  DashbaseConfigCtrl as ConfigCtrl
};
