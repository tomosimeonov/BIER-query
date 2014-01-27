/**
 * New node file
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

var objectCountStopper = function(queryId, prevObjCount, context) {
	setTimeout(function() {

		if (context.queryConfigurations[queryId] !== undefined
				&& context.queryConfigurations[queryId].currentNumObjects === prevObjCount) {
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
			currentConf.dataReceiverEmitter.emit('data', realData);
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

var updatingStatisticsOnFinishingQuery = function(timeRunning,statists){
	statists.updateStatistics(statists.QUERY_PERF, timeRunning);
	statists.updateStatistics(statists.FIN_QUERY);
};

module.exports.setTimeouts = setTimeouts;
module.exports.objectCountStopper = objectCountStopper;
module.exports.removeQueryFromMemory = removeQueryFromMemory;
module.exports.filterData = filterData;
module.exports.processingQueryData = processingQueryData;
module.exports.updatingStatisticsOnFinishingQuery = updatingStatisticsOnFinishingQuery;