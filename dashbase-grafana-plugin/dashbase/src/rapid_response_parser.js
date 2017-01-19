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

		if (sentTargets[0].type == "timeseries") { // graph format

			target = this.response.data.aggregations[sentTargets[0].alias]; // change to i for when implementing support for multi queries per graph
			if (!target) { 
				this.response.data = []; // no aggregation response, likely due to no data within timerange
				return this.response;
			}

			// HISTOGRAM RESPONSE
			if (target.histogramBuckets) {
				var buckets = target.histogramBuckets;
				dataArr.push({
					"target": sentTargets[0].alias,
					"datapoints": _.map(buckets, bucket => {
						return [bucket.count, bucket.timeInSec * 1000];
					}) 
				});
			}


		} else { // table format

			// HITS RESPONSE
			var hits = this.response.data.hits;
			if ((this.response.data.numHits == 0 && this.response.data.hits.length == 0)
			 && _.isEmpty(this.response.data.aggregations)) { // no hits or aggregations
				this.response.data = [];
				return this.response;
			}
			var fields = Object.keys(hits[0].payload.fields); // take the first hit and extract fields
			var columns = [
			{
				"text": "DATE",
				"type": "date",
				"sort": false
			}
			];
			columns = columns.concat(_.map(fields, field => {
				return {"text": field.toUpperCase()}
			}));
			columns.push({
				"text": "RAW",
				"type": "string",
				"sort": false
			});

			dataArr = [{ 
				"columns": columns,
				"rows": _.map(hits, hit => {
					var row = [hit.timeInSeconds * 1000];
					for (var i = 0; i < fields.length; i++) {
						row.push(hit.payload.fields[fields[i]]);
					}
					row.push(hit.payload.stored);
					return row;
				}),
				"type": "table"
			}];
		}
		this.response.data = dataArr;
		return this.response;
	}
}
