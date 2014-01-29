/**
 * Runner to process sql plans for insert operation.
 */
var insertFunctions = require('../InsertManipulationFuncs');
var schemaFunctions = require('../SchemaManipulationFuncs');
var utilities = require('../Utilities');

function DataInsertSQLPlanRunner(store){
	var self = this instanceof DataInsertSQLPlanRunner ? this : Object.create(DataInsertSQLPlanRunner.prototype);
	self.store = store;
	return self;
};

DataInsertSQLPlanRunner.prototype.executePlan = function(sqlPlan,emiter){
	var that = this;
	getPrimaryKeyNameForSchema(sqlPlan['INSERT INTO'].table, function(err, keyName) {
		if (err || keyName === undefined || keyName === "")
			utilities.emitError(emiter, new Error('Problem finding primary key try later'));
		else
			insertFunctions.decodeInsertStatementPlan(sqlPlan, keyName, function(err, decodedData) {
				if (err) {
					utilities.emitError(emiter, err);
				} else {
					that.insertData(decodedData.table, decodedData.key, decodedData.columns,
							decodedData.values, decodedData.keepAliveValue, emiter);
				}
			});
	});
};

DataInsertSQLPlanRunner.prototype.insertData = function(namespace, key, columns, values, keepAlive, emiter) {
	var that = this;
	if (key === undefined || keepAlive === undefined)
		utilities.emitError(emiter, new Error("No primary key value or keep alive provided"));
	else {
		// TODO get schema and properties correctly
		var schema = columns;
		var schemaPropNames = columns;

		isSatisfingSchema(columns, schema, function(err, schemaComplied) {
			if (schemaComplied) {
				var result = [];
				var length = schemaPropNames.length;
				for (var i = 0; i < length; i++) {
					if (columns.indexOf(schemaPropNames[i]) > -1) {
						result[schemaPropNames[i]] = values[columns.indexOf(schemaPropNames[i])];
					} else {
						result[schemaPropNames[i]] = undefined;
					}
				}

				that.store(namespace, key, values, keepAlive, function(data) {
					utilities.emitSuccess(emiter, data);
				});
			} else
				utilities.emitError(emiter, new Error("Used columns not in the schema"));
		});
	}
};

//TODO Create Utilities file
var isSatisfingSchema = function(columns, schema,callback){
	schemaFunctions.isSatisfingSchema(columns, schema, callback);
};

var getPrimaryKeyNameForSchema = function(namespace,callback){
	// TODO Get primary key from schema
	// callback(new Error("No key"), undefined)
	callback(undefined, "CustomerName");
};

module.exports.DataInsertSQLPlanRunner = DataInsertSQLPlanRunner;