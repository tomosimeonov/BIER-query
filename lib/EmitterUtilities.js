/**
 * Helper methods for Emitters
 */

var constants = require('./Constants');

var emitError = function(emiter, err) {
	emiter.emit(ERROR, err);
};

var emitSuccess = function(emiter, data) {
	emiter.emit(constants.SUCCESS, data);
};

var emitData = function(emiter, err) {
	emiter.emit(constants.DATA, err);
};

var emitQueryFinish = function(emiter) {
	emiter.emit(constants.QUERY_FINISH);
};

var emitExecuting = function(emiter,queryId){
	emiter.emit(constants.EXECUTING,queryId);
};

module.exports.emitSuccess = emitSuccess;
module.exports.emitError = emitError;
module.exports.emitData = emitData;
module.exports.emitQueryFinish = emitQueryFinish;
module.exports.emitExecuting = emitExecuting;