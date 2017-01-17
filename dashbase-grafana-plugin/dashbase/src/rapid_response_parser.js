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
		console.log(sentTargets);
		if (sentTargets[0].type == "timeseries") { // graph format
			console.log("YOLO");
			target = this.response.data.aggregations[sentTargets[0].alias]; // change to i for when implementing support for multi queries per graph
			if (!target) return this.response; // no response from bad query
			if (target.histogramBuckets) {
				var buckets = target.histogramBuckets;
				dataArr.push({
					"target": sentTargets[0].alias,
					"datapoints": _.map(buckets, bucket => {
						return [bucket.count, bucket.timeInSec * 1000]
					}) 
				});
			}
		} else { // table format
			var hits = this.response.data.hits;
			console.log(hits);
			dataArr = [{
				"columns": [
				{
					"text": "DATE",
					"type": "date",
					"sort": true,
					"desc": true,
				},
				{
					"text": "RESPONSE"
				},
				{
					"text": "HOST"
				},
				{
					"text": "BYTESSENT",
					"sort": true
				},
				{
					"text": "RAW"
				}
				],
				"rows": _.map(hits, hit => {
					return [hit.timeInSec, hit.payload.fields.response[0], hit.payload.fields.host[0], hit.payload.fields.bytesSent, hit.payload.stored]
				}),
				"type": "table"
			}
			];
		}
		console.log(dataArr);
		this.response.data = dataArr;
		return this.response;
	}
}
