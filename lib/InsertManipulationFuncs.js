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


var areValuesSatisfingSchema = function(columns,values,schema,callback){
	var properties = schema.properties;
	var ans = true;
	
	for(var i = 0; i < columns.size; i++){
		if(typeof values[i] !== properties[columns[i]].type) ans = false;
	}
	for(key in properties){
		if(properties[key].notNull && columns.indexOf(key) !== -1) ans = false; 
	}
	callback(undefined,ans);
	
};

module.exports.decodeInsertStatementPlan = decodeInsertStatementPlan;
module.exports.areValuesSatisfingSchema = areValuesSatisfingSchema;