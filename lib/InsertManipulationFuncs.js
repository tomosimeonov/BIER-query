/**
 * New node file
 */

var decodeInsertStatementPlan = function(insertPlan, keyName, callback) {
	var data = [];
	data.table = insertPlan['INSERT INTO'].table;
	data.columns = insertPlan['INSERT INTO'].columns;
	data.values = insertPlan.VALUES[0];

	if (data.columns.indexOf(keyName) == -1)
		callback(new Error('No primary key provided.'), undefined);
	else {
		data.key = data.values[data.columns.indexOf(keyName)];
		var keepAliveIndex = data.columns.indexOf('KEEP_ALIVE');
		var keepAliveValue = undefined;
		if (keepAliveIndex > -1 && typeof data.values[keepAliveIndex] != 'number') {
			keepAliveValue = data.values[keepAliveIndex];
			data.columns.splice(keepAliveIndex, 1);
			data.values.splice(keepAliveIndex, 1);
			data.keepAliveValue = keepAliveValue;
			callback(undefined, data);
		} else {
			callback(new Error('No KEEP_ALIVE'), undefined);
		}
	}
};

module.exports.decodeInsertStatementPlan = decodeInsertStatementPlan;