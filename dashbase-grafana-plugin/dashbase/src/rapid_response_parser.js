import _ from 'lodash';

export class RapidResponseParser {
	constructor(targets, response) {
		this.targets = targets;
		this.response = response;
	}

	parseGraphResponse(sentTargets) {
		var target;
		var dataArr = [];
		for (var i = 0; i < sentTargets.length; i++) {
			target = this.response.data.aggregations[sentTargets[0].alias]; // change to i for when implementing support for multi queries per graph
			
			if (target.histogramBuckets) {
				var buckets = target.histogramBuckets;
				dataArr.push({
					"target": sentTargets[i].alias,
					"datapoints": _.map(buckets, bucket => {
						return [bucket.count, bucket.timeInSec * 1000]// convert sec to ms
					}) 
				});
			}
		}
		this.response.data = dataArr;
		return this.response;
	}
}
