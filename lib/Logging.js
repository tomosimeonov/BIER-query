/**
 * Logging helper methods
 * 
 * @author Tomo Simeonov
 */

var constants = require('./Constants');

/**
 * 	QUERY LAYER 
 */

/**
 * Log that a SQL string was parsed
 */
var logSqlParsedEvent = function(logHolder, err, queryId) {

	if (err) {
		logHolder.emitEvent(constants.LOG_INFO, [ trimQueryId(queryId), "Query was not parsed due to error." ]);
		logHolder.emitEvent(constants.LOG_FINE, [ trimQueryId(queryId), "Query was not parsed due to error: " + err ]);
	} else {
		logHolder.emitEvent(constants.LOG_INFO, [ trimQueryId(queryId), "Query has been parsed." ]);
	}
};

/**
 * Log that a validation of data was done
 */
var logValidationPerformedEvent = function(logHolder, err, passedIt, queryId) {
	if (err) {
		logHolder.emitEvent(constants.LOG_INFO, [ trimQueryId(queryId), "Query was not validated due to error." ]);
		logHolder.emitEvent(constants.LOG_FINE,
				[ trimQueryId(queryId), "Query was not validated due to error: " + err ]);
	} else {
		logHolder.emitEvent(constants.LOG_INFO, [ trimQueryId(queryId), "Validation of query was performed." ]);
		if(passedIt === true){
			logHolder.emitEvent(constants.LOG_FINE, [ trimQueryId(queryId), "Validation of query was successful." ]);
		}else{
			logHolder.emitEvent(constants.LOG_FINE, [ trimQueryId(queryId), "Validation of query was unsuccessful." ]);
		}
		
	}
};

// Helper method to trip a queryId
var trimQueryId = function(queryId) {
	var start = 0;
	if (queryId.length > 20) {
		start = queryId.length - 20;
	}
	return queryId.substring(start, queryId.length);
};

module.exports.logSqlParsedEvent = logSqlParsedEvent;
module.exports.logValidationPerformedEvent = logValidationPerformedEvent;