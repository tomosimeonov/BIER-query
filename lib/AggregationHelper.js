/**
 * Functions to help process aggregations
 * 
 * @author Tomo Simeonov
 */

var constants = require('./Constants');
var underscore = require('underscore');

function AggregationHelper() {
	var self = this instanceof AggregationHelper ? this : Object.create(AggregationHelper.prototype);
	return self;
};

/**
 * Extract all properties from the select part of the plan and transform them to
 * properties and aggregations
 * 
 * @param selectProperties
 * @param callback
 */
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

	// Dictionary to return
	var processedProperty = {
		'prop' : [],
		'aggreg' : []
	};

	// Only to do it if it is not *
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

/**
 * Method to processed data, e.g add aggregations
 * 
 * @param data
 * @param properties
 * @param aggregs
 * @param nodeId
 * @param callback
 */
AggregationHelper.prototype.processData = function(data, properties, aggregs, nodeId, callback) {

	var tempData = underscore.values(data);
	var buildId = function(typ, name) {
		return typ + "(" + name + ")";
	};

	var createAggregationData = function(tempData, lcallback) {
		var aggregationData = tempData.reduce(function(previousValue, currentValue, index, array) {
			aggregs.forEach(function(el) {
				var id = buildId(el.typ, el.prop);
				if (previousValue[id]) {
					switch (el.typ) {
					case constants.MAX_AGGREGATION:

						if (previousValue[id] < currentValue[el.prop]) {
							previousValue[id] = currentValue[el.prop];
						}

						break;
					case constants.MIN_AGGREGATION:

						if (previousValue[id] > currentValue[el.prop]) {
							previousValue[id] = currentValue[el.prop];
						}

						break;
					case constants.COUNT_AGGREGATION:

						previousValue[id] = previousValue[id] + 1;

						break;
					case constants.SUM_AGGREGATION:

						previousValue[id] = previousValue[id] + currentValue[el.prop];

						break;
					case constants.AVR_AGGREGATION:

						previousValue[id] = previousValue[id] + currentValue[el.prop];

						break;
					default:
						break;
					}

				} else {

					previousValue[id] = currentValue[el.prop];
					if (constants.COUNT_AGGREGATION == el.typ) {
						previousValue[id] = 1;
					}

				}
			});
			return previousValue;
		}, {});

		lcallback(aggregationData);
	};

	createAggregationData(tempData, function(aggregationData) {
		// Checks if there are any avr aggregations
		var anyAvr = aggregs.filter(function(el) {
			return el.typ === constants.AVR_AGGREGATION;
		});

		// if there are update the value
		if (anyAvr.length !== 0) {
			var size = Object.keys(data).length;
			anyAvr.forEach(function(el) {
				var id = buildId(el.typ, el.prop);
				aggregationData[id] = aggregationData[id] / size;
			});
		}

		// If not only aggregation create new data
		if (properties.length !== aggregs.length) {
			var count = function(array, value) {
				return array.filter(function(elem) {
					return elem === value;
				}).length;
			};

			var result = {};

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
				result[key] = test(data[key]);
			}

			callback(undefined, result);
		} else {
			callback(undefined, {
				nodeId : aggregationData
			});
		}
	});

};

module.exports.AggregationHelper = AggregationHelper;
