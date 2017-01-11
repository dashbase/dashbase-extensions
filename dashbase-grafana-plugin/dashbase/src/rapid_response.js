export class RapidResponse {
	constructor(targets, response) {
		this.targets = targets;
		this.response = response;
	}

	parseResponse() {
		var target = this.response.data.aggregations['ts_day'];
		var result = target.histogramBuckets;
		this.response.data = [{
			"target": 'ts_day',
    	"datapoints":[
      [result[0].count, result[0].timeInSec * 1000] // convert sec to ms
    ]
		}];
		return this.response;
	}
}