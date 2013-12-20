/**
 * New node file
 */

function AggregationHelper() {
	var self = this instanceof AggregationHelper ? this : Object.create(AggregationHelper.prototype);
	return self;
};

AggregationHelper.prototype.MAX_AGGREGATION = 'MAX';
AggregationHelper.prototype.MIN_AGGREGATION = 'MIN';
AggregationHelper.prototype.COUNT_AGGREGATION = 'COUNT';
AggregationHelper.prototype.SUM_AGGREGATION = 'SUM';
AggregationHelper.prototype.AVR_AGGREGATION = 'AVR';

AggregationHelper.prototype.propertiesProcessor = function(selectProperties, callback) {
	var that = this;
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
			case that.MAX_AGGREGATION:
				var normalized = normalize(element, that.MAX_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, that.MAX_AGGREGATION);
				break;
			case that.MIN_AGGREGATION:
				var normalized = normalize(element, that.MIN_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, that.MIN_AGGREGATION);
				break;
			case that.COUNT_AGGREGATION:
				var normalized = normalize(element, that.COUNT_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, that.COUNT_AGGREGATION);
				break;
			case that.SUM_AGGREGATION:
				var normalized = normalize(element, that.SUM_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, that.SUM_AGGREGATION);
				break;
			case that.AVR_AGGREGATION:
				var normalized = normalize(element, that.AVR_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, that.AVR_AGGREGATION);
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

AggregationHelper.prototype.processData = function(data, properties, aggregs, callback) {
	var that = this;
	var tempData = data;
	var buildId = function(typ, name) {
		return typ + "(" + name + ")";
	};

	var aggregationData = tempData.reduce(function(previousValue, currentValue, index, array) {
		aggregs.forEach(function(el) {
			var id = buildId(el.typ, el.prop);
			if (previousValue[id]) {
				switch (el.typ) {
				case that.MAX_AGGREGATION:
					if (previousValue[id] < currentValue[el.prop]) {
						previousValue[id] = currentValue[el.prop];
					}
					break;
				case that.MIN_AGGREGATION:
					if (previousValue[id] > currentValue[el.prop]) {
						previousValue[id] = currentValue[el.prop];
					}
					break;
				case that.COUNT_AGGREGATION:
					previousValue[id] = previousValue[id] + 1;
					break;
				case that.SUM_AGGREGATION:
					previousValue[id] = previousValue[id] + currentValue[el.prop];
					break;
				case that.AVR_AGGREGATION:
					previousValue[id] = previousValue[id] + currentValue[el.prop];
					break;
				default:
					break;
				}

			} else {
				previousValue[id] = currentValue[el.prop];
				if (that.COUNT_AGGREGATION === el.typ) {
					previousValue[id] = 1;
				}
			}
		});
		return previousValue;
	}, {});
	var anyAvr = aggregs.filter(function(el) {
		return el.typ === that.AVR_AGGREGATION;
	});

	if (anyAvr.length !== 0) {
		var size = data.length;
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

		var ret = data.map(function(oneData) {
			aggregs.forEach(function(el) {

				var id = buildId(el.typ, el.prop);
				if (count(properties, el.prop) === 1) {
					delete oneData[el.prop];
				}
				oneData[id] = aggregationData[id];
			});
			return oneData;
		});
		callback(undefined, ret);
	} else {
		callback(undefined, [ aggregationData ]);
	}

};

module.exports.AggregationHelper = AggregationHelper;
