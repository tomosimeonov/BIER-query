/**
 * Common methods between executors
 * 
 * @author Tomo Simeonov
 */
var underscore = require('underscore');
var emitterUtilities = require('../../EmitterUtilities');
var logging = require('./Logging');
var constants = require('../../Constants');

/**
 * Set when the query to timeout
 */
var setTimeouts = function(queryId, queryCallback, maxObjects, timeout, context) {
	// Set up object count timeout if any
	if (maxObjects !== -1) {
		objectCountStopper(queryId, 0, context);
		return context.defaultObjectTimeoutInMillis;
	}

	// Set up timeout if any
	if (timeout !== undefined) {
		setTimeout(function() {
			context.finish(queryId, queryCallback);
		}, timeout);
		return timeout;
	}

	// Default timeout if non provided
	if (maxObjects === -1 && timeout === undefined) {
		setTimeout(function() {
			context.finish(queryId, queryCallback);
		}, context.defaultTimeoutInMillis);
		return context.defaultTimeoutInMillis;
	}
};

/**
 * Sets when the query to timeout when the objects count has been reach/ no more
 * data for given time
 */
var objectCountStopper = function(queryId, prevObjCount, context) {
	setTimeout(
			function() {

				if (context.queryConfigurations[queryId] !== undefined
						&& (context.queryConfigurations[queryId].currentNumObjects === prevObjCount || context.queryConfigurations[queryId].currentNumObjects >= context.queryConfigurations[queryId].maxObjects)) {
					context.finish(queryId);
				}
			}, context.defaultObjectTimeoutInMillis);
};

/**
 * Removes query from memory
 */
var removeQueryFromMemory = function(queryId, timeout, context) {
	setTimeout(function() {
		if (context.queryConfigurations[queryId]) {
			delete context.queryConfigurations[queryId];
			delete context.runningData[queryId];
		}
	}, context.queryConfigurations[queryId].configuration.timeoutValue * 5);
};

/**
 * Helper method to filter data for query
 * 
 * @param data
 *            The data to be filtered
 * @param filter
 *            The filter
 * @param transformation
 *            If any transformation of the data after filtering is needed
 * @param properties
 *            Handed to transformation function, if any, with a single data
 *            entry
 * @param callback
 */
var filterData = function(data, filter, transformation, properties, callback) {
	var filteredData = {};
	var size = 0;
	for ( var key in data) {
		if (filter(data[key])) {
			size++;
			if (transformation) {
				filteredData[key] = transformation(data[key], properties);
			} else {
				filteredData[key] = data[key];
			}
		}
	}
	callback(filteredData, size);
};

/**
 * Helper method to return to client relative data from query.
 * 
 * @param queryId
 *            Id of query
 * @param realData
 *            Filtered data
 * @param aggregation
 *            Data set is part of aggregation
 * @param context
 *            The executor object.
 */
var processingQueryData = function(queryId, realData, aggregation, context) {
	if (realData.length !== 0 && context.queryConfigurations[queryId]) {
		var currentConf = context.queryConfigurations[queryId];

		// Add keys in set
		var notPresent = {};
		for ( var key in realData) {
			if (context.queryConfigurations[queryId].keys[key] == undefined) {
				context.queryConfigurations[queryId].keys[key] = true;
				notPresent[key] = realData[key];
			}
		}
		;
		if (Object.keys(notPresent).length > 0) {

			// Check if should emit data or store it for remote/aggregation
			if (currentConf.queryCallback === undefined && !aggregation) {
				logging.logDataDeliveredEvent(context.statisticHolder, Object.keys(notPresent).length,
						context.queryConfigurations[queryId].beginingTime, queryId);

				emitterUtilities.emitData(currentConf.dataReceiverEmitter, underscore.values(notPresent));
			} else {
				context.runningData[queryId] = underscore.extend(context.runningData[queryId], notPresent);
			}

			// Set current number of objects and checks if needs to finish
			currentConf.currentNumObjects = currentConf.currentNumObjects + Object.keys(notPresent).length;
			if (currentConf.currentNumObjects === currentConf.configuration.maxObjects) {
				// Have to terminate
				context.finish(queryId);
			} else if (currentConf.configuration.maxObjects !== -1) {
				// Restart timer for object count
				objectCountStopper(queryId, currentConf.currentNumObjects, context);
			}
		}
	}
};
/**
 * Method to extract schema
 * 
 * @param namespace
 * @param storageLayer
 */
var getschema = function(namespace, storageLayer, callback) {
	storageLayer.Node.getGlobal(namespace, function(schema) {
		if (schema) {
			callback(undefined, schema);
		} else {
			callback(new Error("schema not defined in this node"), undefined);
		}
	});
};

/**
 * Produces unmarshaller to unmarshall stored data
 */
var produceUnmarshallerDataFunction = function(schema, aggregations, callback, secondSchema) {
	var properties = schema.properties;

	if (aggregations) {
		var produceAggName = function(aggregation, prop) {
			return aggregation + "(" + prop + ")";
		};

		for (var i = 0; i < aggregations.length; i++) {
			var id = produceAggName(aggregations[i].typ, aggregations[i].prop);
			properties[id] = {};
			switch (aggregations[i].typ) {
			case constants.MAX_AGGREGATION:
				properties[id].type = properties[aggregations[i].prop].type;
				break;
			case constants.MIN_AGGREGATION:
				properties[id].type = properties[aggregations[i].prop].type;
				break;
			case constants.COUNT_AGGREGATION:
				properties[id].type = "int";
				break;
			case constants.SUM_AGGREGATION:
				properties[id].type = properties[aggregations[i].prop].type;
				break;
			case constants.AVR_AGGREGATION:
				properties[id].type = properties[aggregations[i].prop].type;
				break;
			}
			;
		}
	}

	if (secondSchema) {
		properties = underscore(properties, secondSchema.properties);
	}

	var unmarshaller = function(data, lcallback) {
		var propKeys = Object.keys(properties);
		for ( var index in propKeys) {
			var key = propKeys[index];

			if (properties[key].type === 'int') {
				for ( var i in data) {

					if (data[i][key]) {
						var int = parseInt(data[i][key]);
						data[i][key] = int;

					}

				}
				if (properties[key].type === 'float') {
					for ( var i in data) {

						if (data[i][key]) {
							var float = parseFloat(data[i][key]);
							data[i][key] = float;
						}

					}
				}
			}
		}
		lcallback(data);

	};

	callback(unmarshaller);
};

var sendDataMessage = function(origin, unfineshedMesBuilder, payload, store) {

	var inGroups = 0;
	var keys = Object.keys(payload);

	if (keys.length !== 0) {

		var strData = JSON.stringify(payload[keys[0]]);
		if (strData.length * keys.length > 20000) {
			inGroups = (strData.length * keys.length) / 15000;
			if (keys.length < inGroups) {
				inGroups = keys.length;
			}
		}

		if (inGroups === 0) {
			var message = unfineshedMesBuilder.setPayload(payload).buildMessage();
			store.Node.send(origin, JSON.stringify(message), function(data) {

			});
		} else {
			var bags = Math.round(keys.length / inGroups);

			var currentData = {};
			for (var i = 0; i < keys.length; i++) {
				currentData[keys[i]] = payload[keys[i]];
				if ((i + 1) % bags === 0) {
					var message = unfineshedMesBuilder.setPayload(currentData).buildMessage();
					store.Node.send(origin, JSON.stringify(message), function(data) {
					});
					currentData = {};
				}
			}
			if (Object.keys(currentData).length !== 0) {
				var message = unfineshedMesBuilder.setPayload(currentData).buildMessage();
				store.Node.send(origin, JSON.stringify(message), function(data) {
				});
			}

		}
	}
};
// Common functions
module.exports.setTimeouts = setTimeouts;
module.exports.objectCountStopper = objectCountStopper;
module.exports.removeQueryFromMemory = removeQueryFromMemory;
module.exports.filterData = filterData;
module.exports.processingQueryData = processingQueryData;
module.exports.getschema = getschema;
module.exports.produceUnmarshallerDataFunction = produceUnmarshallerDataFunction;
module.exports.sendDataMessage = sendDataMessage;
