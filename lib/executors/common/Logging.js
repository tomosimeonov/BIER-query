/**
 *  Common logging methods for executors
 *  
 *  @author Tomo Simeonov
 */
var constants = require('../../Constants');
/**
 * Method to provide statistics to the event holder
 */
var updatingStatisticsOnFinishingQuery = function(timeRunning, statistics) {
	statistics.emitEvent(constants.QUERY_PERF, timeRunning);
	statistics.emitEvent(constants.FIN_QUERY);
};

/**
 * Logging for message received
 */
var logMessageReceivedEvent = function(logHolder, message, queryId) {

	logHolder.emitEvent(constants.LOG_INFO, [ trimQueryId(queryId), "Message received." ]);

	logHolder.emitEvent(constants.LOG_FINE, [ trimQueryId(queryId), "Message received from " + message.origin ]);

	if (message.payloadType === constants.DATA_MESSAGE) {
		logHolder.emitEvent(constants.LOG_FINER, [
				trimQueryId(queryId),
				"Message received from " + message.origin + " contains data of size "
						+ Object.keys(message.payload).length + " tuples" ]);
	}
	if (message.payloadType === constants.CONFIG_MESSAGE) {
		logHolder.emitEvent(constants.LOG_FINER,
				[ trimQueryId(queryId), "Message received is to execute remote query" ]);
	}

	if (message.payloadType === constants.PAYLOAD_TYPE_DATA_SEARCH) {
		logHolder.emitEvent(constants.LOG_FINER, [ trimQueryId(queryId),
				"Message received from " + message.origin + " is to execute local data search only" ]);
	}

	if (message.payloadType === constants.SEC_DATA) {
		logHolder.emitEvent(constants.LOG_FINER, [
				trimQueryId(queryId),
				"Message received from " + message.origin + " contains data for second table of join query of size "
						+ Object.keys(message.payload).length + " tuples" ]);
	}

};

/**
 * Logging for local filtering
 */
var logLocalDataAccessEvent = function(logHolder, namespace, dataSize, queryId) {
	logHolder.emitEvent(constants.LOG_INFO, [ trimQueryId(queryId), "Local storage accessed." ]);

	logHolder.emitEvent(constants.LOG_FINE,
			[ trimQueryId(queryId), "Extracting local data for namespace " + namespace ]);

	logHolder.emitEvent(constants.LOG_FINER, [ trimQueryId(queryId),
			"Extracting local data for namespace " + namespace + " and accessed " + dataSize + " objects " ]);

};

/**
 * Logging for data delivering to client
 * 
 */
var logDataDeliveredEvent = function(logHolder, dataSize, startTime, queryId) {
	logHolder.emitEvent(constants.LOG_INFO, [ trimQueryId(queryId), "Data delivered." ]);

	var currentTime = ((new Date()) - startTime) / 1000;

	logHolder.emitEvent(constants.LOG_FINE, [ trimQueryId(queryId),
			"Data of size " + dataSize + " delivered to requester." ]);
	logHolder.emitEvent(constants.LOG_FINER, [ trimQueryId(queryId),
			"Data of size " + dataSize + " delivered " + currentTime + " seconds after initial request to requester" ]);

};

/**
 * Logging for distributing configuration
 */
var logDistributingConfigurationEvent = function(logHolder, destinations, queryId) {

	logHolder.emitEvent(constants.LOG_INFO, [ trimQueryId(queryId), "Distributing configuration" ]);
	if (destinations.length !== 0) {
		logHolder.emitEvent(constants.LOG_FINE, [ trimQueryId(queryId),
				"Distributing configuration using direct messaging." ]);
		logHolder.emitLogFiner(constants.LOG_FINER, [ trimQueryId(queryId),
				"Distributing configuration using direct messaging to " + destinations ]);
	} else {
		logHolder
				.emitEvent(constants.LOG_FINE, [ trimQueryId(queryId), "Distributing configuration using broadcast." ]);
	}

};

/**
 * Logging for missing schema
 */
var logRemoteErrorOnMissingschema = function(logHolder, namespace, queryId) {
	logHolder.emitEvent(constants.LOG_INFO, [ trimQueryId(queryId), "ERROR: Cannot find schema for " + namespace ]);
};

// Helper method to trip a queryId
var trimQueryId = function(queryId) {
	var start = 0;
	if (queryId.length > 20) {
		start = queryId.length - 20;
	}
	return queryId.substring(start, queryId.length);
};

// Statistics
module.exports.updatingStatisticsOnFinishingQuery = updatingStatisticsOnFinishingQuery;

// Logging
module.exports.logMessageReceivedEvent = logMessageReceivedEvent;
module.exports.logLocalDataAccessEvent = logLocalDataAccessEvent;
module.exports.logDataDeliveredEvent = logDataDeliveredEvent;
module.exports.logDistributingConfigurationEvent = logDistributingConfigurationEvent;
module.exports.logRemoteErrorOnMissingschema = logRemoteErrorOnMissingschema;