/**
 * Common methods between executors
 */
var emitterUtilities = require('../EmitterUtilities');

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

var objectCountStopper = function(queryId, prevObjCount, context) {
	setTimeout(
			function() {

				if (context.queryConfigurations[queryId] !== undefined
						&& (context.queryConfigurations[queryId].currentNumObjects === prevObjCount || context.queryConfigurations[queryId].currentNumObjects >= context.queryConfigurations[queryId].maxObjects)) {
					context.finish(queryId);
				}
			}, context.defaultObjectTimeoutInMillis);
};

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
 */
var filterData = function(queryId, data, filter, callback) {
	var realData = data.filter(function(elem) {
		return filter(elem);
	});

	callback(queryId, realData);
};

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
		if (currentConf.queryCallback === undefined && !aggregation) {
			logDataDeliveredEvent(context.statisticHolder, realData.length,
					context.queryConfigurations[queryId].beginingTime, queryId);
			emitterUtilities.emitData(currentConf.dataReceiverEmitter, realData);
		} else {
			context.runningData[queryId] = context.runningData[queryId].concat(realData);
		}

		currentConf.currentNumObjects = currentConf.currentNumObjects + realData.length;
		if (currentConf.currentNumObjects === currentConf.configuration.maxObjects) {
			// Have to terminate
			context.finish(queryId);
		} else if (currentConf.configuration.maxObjects !== -1) {
			// Restart timer for object count
			objectCountStopper(queryId, currentConf.currentNumObjects, context);
		}
	}
};

var updatingStatisticsOnFinishingQuery = function(timeRunning, statists) {
	statists.emitEvent(statists.QUERY_PERF, timeRunning);
	statists.emitEvent(statists.FIN_QUERY);
};

/**
 * Logging for message received
 */
var logMessageReceivedEvent = function(logHolder, message, queryId) {

	logHolder.emitEvent(logHolder.LOG_INFO, [ trimQueryId(queryId), "Message received." ]);

	if (logHolder.isFineLogLevel()) {
		logHolder.emitEvent(logHolder.LOG_FINE, [ trimQueryId(queryId), "Message received from " + message.origin ]);
	}

	if (logHolder.isFinerLogLevel()) {
		if (message.payloadType === "DATA") {
			logHolder.emitEvent(logHolder.LOG_FINER, [ trimQueryId(queryId),
					"Message received contains data of size " + message.payload.length + " tuples" ]);
		}
		if (message.payloadType === "CONFIG") {
			logHolder.emitEvent(logHolder.LOG_FINER, [ trimQueryId(queryId),
					"Message received is to execute remote query" ]);
		}

		if (message.payloadType === "DATASEARCH") {
			logHolder.emitEvent(logHolder.LOG_FINER, [ trimQueryId(queryId),
					"Message received is to execute local data search only" ]);
		}

		if (message.payloadType === "SEC_DATA") {
			logHolder.emitEvent(logHolder.LOG_FINER, [
					trimQueryId(queryId),
					"Message received contains data for second table of join query of size " + message.payload.length
							+ " tuples" ]);
		}
	}
	;
};

/**
 * Logging for local filtering
 */
var logLocalDataAccessEvent = function(logHolder, namespace, dataSize, queryId) {
	logHolder.emitEvent(logHolder.LOG_INFO, [ trimQueryId(queryId), "Local storage accessed." ]);

	if (logHolder.isFineLogLevel()) {
		logHolder.emitEvent(logHolder.LOG_FINE, [ trimQueryId(queryId),
				"Extracting local data for namespace " + namespace ]);
	}
	if (logHolder.isFinerLogLevel()) {
		logHolder.emitEvent(logHolder.LOG_FINER, [ trimQueryId(queryId),
				"Extracting local data for namespace " + namespace + " and accessed " + dataSize + " objects " ]);
	}
};

/**
 * Logging info for data delivering to client
 * 
 */
var logDataDeliveredEvent = function(logHolder, dataSize, startTime, queryId) {
	logHolder.emitEvent(logHolder.LOG_INFO, [ trimQueryId(queryId), "Data delivered." ]);

	var currentTime = ((new Date()) - startTime) / 1000;
	if (logHolder.isFineLogLevel()) {
		logHolder.emitEvent(logHolder.LOG_FINE, [ trimQueryId(queryId),
				"Data of size " + dataSize + " delivered to requester." ]);
	}
	if (logHolder.isFinerLogLevel()) {
		logHolder.emitEvent(logHolder.LOG_FINER, [
				trimQueryId(queryId),
				"Data of size " + dataSize + " delivered " + currentTime
						+ " seconds after initial request to requester" ]);
	}
};

// Helper method to trip a queryId
var trimQueryId = function(queryId) {
	var start = 0;
	if (queryId.length > 10) {
		start = queryId.length - 10;
	}
	return queryId.substring(start, queryId.length);
};

module.exports.setTimeouts = setTimeouts;
module.exports.objectCountStopper = objectCountStopper;
module.exports.removeQueryFromMemory = removeQueryFromMemory;
module.exports.filterData = filterData;
module.exports.processingQueryData = processingQueryData;
module.exports.updatingStatisticsOnFinishingQuery = updatingStatisticsOnFinishingQuery;
module.exports.logMessageReceivedEvent = logMessageReceivedEvent;
module.exports.logLocalDataAccessEvent = logLocalDataAccessEvent;
module.exports.logDataDeliveredEvent = logDataDeliveredEvent;
