/**
 * New node file
 */

var setTimeouts = function(queryId, queryCallback, maxObjects, timeout, context) {
	// Set up object count timeout if any
	if (maxObjects !== -1) {
		objectCountStopper(queryId, 0, queryCallback, context);
		return context.defaultObjectTimeoutInMillis;
	}

	// Set up timeout if any
	if (timeout !== undefined) {
		setTimeout(function() {
			context.returnInformation(queryId, queryCallback);
		}, timeout);
		return timeout;
	}

	// Default timeout if non provided
	if (maxObjects === -1 && timeout === undefined) {
		setTimeout(function() {
			context.returnInformation(queryId, queryCallback);
		}, context.defaultTimeoutInMillis);
		return context.defaultTimeoutInMillis;
	}
};

var objectCountStopper = function(queryId, prevObjCount, queryCallback, context) {
	setTimeout(function() {
		if (context.queryConfigurations[queryId] !== undefined
				&& context.queryConfigurations[queryId].currentNumObjects === prevObjCount)
			context.returnInformation(queryId, queryCallback);
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

module.exports.setTimeouts = setTimeouts;
module.exports.objectCountStopper = objectCountStopper;
module.exports.removeQueryFromMemory = removeQueryFromMemory;