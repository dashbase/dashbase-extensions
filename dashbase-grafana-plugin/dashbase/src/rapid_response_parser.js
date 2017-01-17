import _ from 'lodash';

export class RapidResponseParser {
	constructor(response) {
		this.response = response;
	}

	parseResponse(sentTargets) {
		var target;
		var dataArr = [];
		if (this.response.data.error) {

			// TODO: send error to Grafana front end instead of logging to console
			console.log(this.response.data.error); // log out parse error
			return this.response;
		}
		for (var i = 0; i < sentTargets.length; i++) {
			if (sentTargets[i].type == "timeseries") {
				target = this.response.data.aggregations[sentTargets[0].alias]; // change to i for when implementing support for multi queries per graph
				if (!target) return this.response; // no response from bad query
				if (target.histogramBuckets) {
					var buckets = target.histogramBuckets;
					dataArr.push({
						"target": sentTargets[i].alias,
						"datapoints": _.map(buckets, bucket => {
							return [bucket.count, bucket.timeInSec * 1000]
						}) 
					});
				}
			} else {
				target = this.response.data.hits;
			}
		}
		this.response.data = dataArr;
		return this.response;
	}
}
