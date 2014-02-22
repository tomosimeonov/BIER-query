/**
 * This object is wrapping around PHT interface
 * 
 * @author Tomo Simeonov
 */

var constants = require('./Constants');

function PHTWrapper(phtInterface) {
	var self = this instanceof PHTWrapper ? this : Object.create(PHTWrapper.prototype);
	self.phtInterface = phtInterface;
	return self;
}

/**
 * Executes search in the PHT, if it is unavailable PASS_NO_INDEX_TAG is return
 * as only element of the ids array
 * 
 * @param namespace
 *            The namespace
 * @param key
 *            The key
 * @param value
 *            The value to look for
 * @param operation
 *            The operation =, !=, <>, <=, >=
 * @param callback
 *            Gives a err(undefined,err) and data(undefined,[..])
 */
PHTWrapper.prototype.executeSearch = function(namespace, key, value, operation, callback) {
	switch (operation) {
	default:
		callback(undefined, [ constants.PASS_NO_INDEX_TAG ]);
	}

};

// Create table name for index
var createPropertyTableName = function(namespace, key) {
	return "index-" + namespace + "-" + key;
};

// EXPORT
module.exports.PHTWrapper = PHTWrapper;