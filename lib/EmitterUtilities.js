/**
 * Helper methods for Emitters
 */

var SUCCESS = "SUCCESS";
var DATA = "DATA";
var QUERY_FINISH = "FINISHED";
var ERROR = "ERROR";

var emitError = function(emiter, err) {
	emiter.emit(ERROR, err);
};

var emitSuccess = function(emiter, data) {
	emiter.emit(SUCCESS, data);
};

var emitData = function(emiter, err) {
	emiter.emit(DATA, err);
};

var emitQueryFinish = function(emiter) {
	emiter.emit(QUERY_FINISH);
};

module.exports.SUCCESS = SUCCESS;
module.exports.Data = DATA;
module.exports.QUERY_FINISH = QUERY_FINISH;
module.exports.ERROR = ERROR;

module.exports.emitSuccess = emitSuccess;
module.exports.emitError = emitError;
module.exports.emitData = emitData;
module.exports.emitQueryFinish = emitQueryFinish;
