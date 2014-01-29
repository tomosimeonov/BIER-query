/**
 * New node file
 */
var emitError = function(emiter, err) {
	emiter.emit("err", err);
};

var emitSuccess = function(emiter, success) {
	emiter.emit("success", success);
};

module.exports.emitSuccess = emitSuccess;
module.exports.emitError = emitError;