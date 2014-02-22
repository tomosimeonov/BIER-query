/**
 * Functions to help process aggregations
 */

var constants = require('./Constants');
var underscore = require('underscore');

function AggregationHelper() {
	var self = this instanceof AggregationHelper ? this : Object.create(AggregationHelper.prototype);
	return self;
};

AggregationHelper.prototype.propertiesProcessor = function(selectProperties, callback) {
	var normalize = function(prop, begining, end) {
		return prop.slice(begining, end);
	};
	var prepareFuncReturnForAgg = function(prop, aggregation) {
		return {
			'prop' : [ prop ],
			'aggreg' : [ {
				'typ' : aggregation,
				'prop' : prop
			} ]
		};
	};
	var processedProperty = {
		'prop' : [],
		'aggreg' : []
	};

	if (selectProperties.indexOf('*') == -1) {
		processedProperty = selectProperties.map(function(element) {
			var splitedEl = element.split('(');
			switch (splitedEl[0]) {
			case constants.MAX_AGGREGATION:
				var normalized = normalize(element, constants.MAX_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, constants.MAX_AGGREGATION);
				break;
			case constants.MIN_AGGREGATION:
				var normalized = normalize(element, constants.MIN_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, constants.MIN_AGGREGATION);
				break;
			case constants.COUNT_AGGREGATION:
				var normalized = normalize(element, constants.COUNT_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, constants.COUNT_AGGREGATION);
				break;
			case constants.SUM_AGGREGATION:
				var normalized = normalize(element, constants.SUM_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, constants.SUM_AGGREGATION);
				break;
			case constants.AVR_AGGREGATION:
				var normalized = normalize(element, constants.AVR_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, constants.AVR_AGGREGATION);
				break;
			default:
				return {
					'prop' : [ element ],
					'aggreg' : []
				};
				break;
			}
		}).reduce(function(previousValue, currentValue, index, array) {
			return {
				'prop' : previousValue.prop.concat(currentValue.prop),
				'aggreg' : previousValue.aggreg.concat(currentValue.aggreg)
			};
		}, processedProperty);
	}
	callback(undefined, processedProperty);
};

AggregationHelper.prototype.processData = function(data, properties, aggregs, nodeId, callback) {
	var tempData = underscore.values(data);
	var buildId = function(typ, name) {
		return typ + "(" + name + ")";
	};

	var aggregationData = tempData.reduce(function(previousValue, currentValue, index, array) {
		aggregs.forEach(function(el) {
			var id = buildId(el.typ, el.prop);
			if (previousValue[id]) {
				switch (el.typ) {
				case constants.MAX_AGGREGATION:
					if (currentValue[id]) {
						if (previousValue[id] < currentValue[id]) {
							previousValue[id] = currentValue[id];
						}
					} else {
						if (previousValue[id] < currentValue[el.prop]) {
							previousValue[id] = currentValue[el.prop];
						}
					}
					break;
				case constants.MIN_AGGREGATION:
					if (currentValue[id]) {
						if (previousValue[id] > currentValue[id]) {
							previousValue[id] = currentValue[id];
						}
					} else {
						if (previousValue[id] > currentValue[el.prop]) {
							previousValue[id] = currentValue[el.prop];
						}
					}
					break;
				case constants.COUNT_AGGREGATION:
					if (currentValue[id]) {
						previousValue[id] = previousValue[id] + currentValue[id];
					} else {
						previousValue[id] = previousValue[id] + 1;
					}
					break;
				case constants.SUM_AGGREGATION:
					if (currentValue[id]) {
						previousValue[id] = previousValue[id] + currentValue[id];
					} else {
						previousValue[id] = previousValue[id] + currentValue[el.prop];
					}
					break;
				case constants.AVR_AGGREGATION:
					if (currentValue[id]) {
						previousValue[id] = previousValue[id] + currentValue[id];
					} else {
						previousValue[id] = previousValue[id] + currentValue[el.prop];
					}
					break;
				default:
					break;
				}

			} else {
				if (currentValue[id]) {
					previousValue[id] = currentValue[id];
				} else {
					previousValue[id] = currentValue[el.prop];
					if (constants.COUNT_AGGREGATION == el.typ) {
						previousValue[id] = 1;
					}
				}
			}
		});
		return previousValue;
	}, {});
	var anyAvr = aggregs.filter(function(el) {
		return el.typ === constants.AVR_AGGREGATION;
	});

	if (anyAvr.length !== 0) {
		var size = Object.keys(data).length;
		anyAvr.forEach(function(el) {
			var id = buildId(el.typ, el.prop);
			aggregationData[id] = aggregationData[id] / size;
		});
	}
	if (properties.length !== aggregs.length && aggregs.length !== 0) {

		var count = function(array, value) {
			return array.filter(function(elem) {
				return elem === value;
			}).length;
		};

		var ret = {};

		for ( var key in data) {
			var test = function(oneData) {
				aggregs.forEach(function(el) {

					var id = buildId(el.typ, el.prop);
					if (count(properties, el.prop) === 1) {
						delete oneData[el.prop];
					}
					oneData[id] = aggregationData[id];
				});
				return oneData;
			};
			ret[key] = test(data[key]);
		}
		callback(undefined, ret);
	} else {
		callback(undefined, {
			nodeId : aggregationData
		});
	}

};

module.exports.AggregationHelper = AggregationHelper;
