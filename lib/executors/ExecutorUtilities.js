/**
 * Common methods between executors
 */
var underscore = require('underscore');
var emitterUtilities = require('../EmitterUtilities');
var constants = require('../Constants');
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

// TODO FIX TO USE HASH MAPS
/**
 * Helper method to filter data for query
 */
// var filterData = function(queryId, data, filter, callback) {
// var realData = data.filter(function(elem) {
// return filter(elem);
// });
//
// callback(queryId, realData);
// };
var filterData = function(data, filter, transformation,properties, callback) {
	var filteredData = {};
	var size = 0;
	for ( var key in data) {
		if (filter(data[key])) {
			size++;
			if (transformation) {
				filteredData[key] = transformation(data[key],properties);
			} else {
				filteredData[key] = data[key];
			}
		}
	}
	callback(filteredData, size);
};

// TODO FIX TO USE HASH MAPS
/**
 * Helper method to return to client relative data from query.
 * 
 * @param queryId
 *            Id of query
 * @param realData
 *            Filtered data
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
			// = underscore.values(realData);
			if (currentConf.queryCallback === undefined && !aggregation) {
				logDataDeliveredEvent(context.statisticHolder, Object.keys(notPresent).length,
						context.queryConfigurations[queryId].beginingTime, queryId);

				emitterUtilities.emitData(currentConf.dataReceiverEmitter, underscore.values(notPresent));
			} else {
				context.runningData[queryId] = underscore.extend(context.runningData[queryId], notPresent);
			}

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
 * Method to extract scheme
 */
var getscheme = function(namespace, storageLayer, callback) {
	storageLayer.Node.getGlobal(namespace, function(scheme) {
		if (scheme) {
			callback(undefined, scheme);
		} else {
			callback(new Error("scheme not defined in this node"), undefined);
		}
	});
};

/**
 * Method to produce function to unmarshall data when needed.
 */
var produceUnmarshallerDataFunction = function(scheme, aggregations, callback, secondscheme) {
	var properties = scheme.properties;

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

	if (secondscheme) {
		properties = underscore.extend(properties, secondscheme.properties);
	}

	var unmarshaller = function(data, lcallback) {

		var propKeys = Object.keys(properties);
		for ( var index in propKeys) {
			var key = propKeys[index];
			if (properties[key].type === "int") {
				for ( var i in data) {
					if (data[i][key]) {
						var int = parseInt(data[i][key]);
						data[i][key] = int;
					}

				}
			}
			if (properties[key].type === "float") {
				for ( var i in data) {
					if (data[i][key]) {
						var float = parseFloat(data[i][key]);
						data[i][key] = float;
					}

				}
			}
		}
		lcallback(data);
	};

	callback(unmarshaller);
};

/**
 * Method to provide statistics to statistics holder
 */
var updatingStatisticsOnFinishingQuery = function(timeRunning, statistics) {
	statistics.emitEvent(statistics.QUERY_PERF, timeRunning);
	statistics.emitEvent(statistics.FIN_QUERY);
};

/**
 * Logging for message received
 */
var logMessageReceivedEvent = function(logHolder, message, queryId) {

	logHolder.emitEvent(logHolder.LOG_INFO, [ trimQueryId(queryId), "Message received." ]);

	logHolder.emitEvent(logHolder.LOG_FINE, [ trimQueryId(queryId), "Message received from " + message.origin ]);

	if (message.payloadType === "DATA") {
		logHolder.emitEvent(logHolder.LOG_FINER, [
				trimQueryId(queryId),
				"Message received from " + message.origin + " contains data of size " + Object.keys(message.payload).length
						+ " tuples" ]);
	}
	if (message.payloadType === "CONFIG") {
		logHolder.emitEvent(logHolder.LOG_FINER,
				[ trimQueryId(queryId), "Message received is to execute remote query" ]);
	}

	if (message.payloadType === "DATASEARCH") {
		logHolder.emitEvent(logHolder.LOG_FINER, [ trimQueryId(queryId),
				"Message received is to execute local data search only" ]);
	}

	if (message.payloadType === "SEC_DATA") {
		logHolder.emitEvent(logHolder.LOG_FINER, [
				trimQueryId(queryId),
				"Message received from " + message.origin + " contains data for second table of join query of size "
						+ message.payload.length + " tuples" ]);
	}

};

/**
 * Logging for local filtering
 */
var logLocalDataAccessEvent = function(logHolder, namespace, dataSize, queryId) {
	logHolder.emitEvent(logHolder.LOG_INFO, [ trimQueryId(queryId), "Local storage accessed." ]);

	logHolder.emitEvent(logHolder.LOG_FINE,
			[ trimQueryId(queryId), "Extracting local data for namespace " + namespace ]);

	logHolder.emitEvent(logHolder.LOG_FINER, [ trimQueryId(queryId),
			"Extracting local data for namespace " + namespace + " and accessed " + dataSize + " objects " ]);

};

/**
 * Logging info for data delivering to client
 * 
 */
var logDataDeliveredEvent = function(logHolder, dataSize, startTime, queryId) {
	logHolder.emitEvent(logHolder.LOG_INFO, [ trimQueryId(queryId), "Data delivered." ]);

	var currentTime = ((new Date()) - startTime) / 1000;

	logHolder.emitEvent(logHolder.LOG_FINE, [ trimQueryId(queryId),
			"Data of size " + dataSize + " delivered to requester." ]);
	logHolder.emitEvent(logHolder.LOG_FINER, [ trimQueryId(queryId),
			"Data of size " + dataSize + " delivered " + currentTime + " seconds after initial request to requester" ]);

};

var logDistributingConfigurationEvent = function(logHolder, destinations, queryId) {

	logHolder.emitEvent(logHolder.LOG_INFO, [ trimQueryId(queryId), "Distributing configuration" ]);
	if (destinations.length !== 0) {
		logHolder.emitEvent(logHolder.LOG_FINE, [ trimQueryId(queryId),
				"Distributing configuration using direct messaging." ]);
		logHolder.emitLogFiner(logHolder.LOG_FINER, [ trimQueryId(queryId),
				"Distributing configuration using direct messaging to " + destinations ]);
	} else {
		logHolder
				.emitEvent(logHolder.LOG_FINE, [ trimQueryId(queryId), "Distributing configuration using broadcast." ]);
	}

};

var logRemoteErrorOnMissingscheme = function(logHolder, namespace, queryId) {
	logHolder.emitEvent(logHolder.LOG_INFO, [ trimQueryId(queryId), "ERROR: Cannot find scheme for " + namespace ]);
};

// Helper method to trip a queryId
var trimQueryId = function(queryId) {
	var start = 0;
	if (queryId.length > 20) {
		start = queryId.length - 20;
	}
	return queryId.substring(start, queryId.length);
};

// Common functions
module.exports.setTimeouts = setTimeouts;
module.exports.objectCountStopper = objectCountStopper;
module.exports.removeQueryFromMemory = removeQueryFromMemory;
module.exports.filterData = filterData;
module.exports.processingQueryData = processingQueryData;
module.exports.getscheme = getscheme;
module.exports.produceUnmarshallerDataFunction = produceUnmarshallerDataFunction;
// Statistics
module.exports.updatingStatisticsOnFinishingQuery = updatingStatisticsOnFinishingQuery;

// Logging
module.exports.logMessageReceivedEvent = logMessageReceivedEvent;
module.exports.logLocalDataAccessEvent = logLocalDataAccessEvent;
module.exports.logDataDeliveredEvent = logDataDeliveredEvent;
module.exports.logDistributingConfigurationEvent = logDistributingConfigurationEvent;
module.exports.logRemoteErrorOnMissingscheme = logRemoteErrorOnMissingscheme;
