/**
 * Runner to process sql plans for insert operation.
 */
var schemaFunctions = require('../SchemaManipulationFuncs');
var utilities = require('../EmitterUtilities');

function DataInsertSQLPlanRunner(store, eventHolder) {
	var self = this instanceof DataInsertSQLPlanRunner ? this : Object.create(DataInsertSQLPlanRunner.prototype);
	self.store = store;
	self.eventHolder = eventHolder;
	self.counter = 0;
	return self;
};

DataInsertSQLPlanRunner.prototype.executePlan = function(sqlPlan, emiter) {

	var that = this;

	var queryId = "insert-" + this.counter;
	this.counter++;

	utilities.emitExecuting(emiter, queryId);
	this.eventHolder.emitEvent("LOG_INFO", [ queryId, "Inserting data into the database" ]);

	getPrimaryKeyNameForSchema(sqlPlan['INSERT INTO'].table, function(err, keyName) {
		if (err || keyName === undefined || keyName === "")
			utilities.emitError(emiter, new Error('Problem finding primary key try later'));
		else
			that.decodeInsertStatementPlan(sqlPlan, keyName, function(err, decodedData) {

				if (err) {
					utilities.emitError(emiter, err);
				} else {
					that.insertData(decodedData.table, decodedData.key, decodedData.columns, decodedData.values,
							decodedData.keepAliveValue, emiter, queryId);
				}
			}, queryId);
	});
};

DataInsertSQLPlanRunner.prototype.insertData = function(namespace, key, columns, values, keepAlive, emiter, queryId) {
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

				that.eventHolder.emitEvent("LOG_FINER", [ queryId, "Sending the new data to the storage layer." ]);
				that.store(namespace, key, values, keepAlive, function(data) {
					that.eventHolder.emitEvent("LOG_FINER", [ queryId, "Storage layer finished execution." ]);
					utilities.emitSuccess(emiter, data);
					utilities.emitQueryFinish(emiter);
				});
			} else
				utilities.emitError(emiter, new Error("Used columns not in the schema"));
		});
	}
};

DataInsertSQLPlanRunner.prototype.decodeInsertStatementPlan = function(insertPlan, keyName, callback, queryId) {
	this.eventHolder.emitEvent("LOG_FINE", [ queryId, "Transforming sql data to internal structure" ]);

	var data = {};
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

			var tempvalues = {};
			for (var i = 0; i < data.columns.length; i++) {
				tempvalues[data.columns[i]] = data.values[i];
			}
			data.values = tempvalues;
			callback(undefined, data);
		} else {
			callback(new Error('No KEEP_ALIVE'), undefined);
		}
	}
};

DataInsertSQLPlanRunner.prototype.areValuesSatisfingSchema = function(columns, values, schema, callback, queryId) {

	this.eventHolder.emitEvent("LOG_FINE", [ queryId, "Checking if values satisfies schema" ]);
	this.eventHolder.emitEvent("LOG_FINER",
			[ queryId, "Checking " + values.size + " values if values satisfies schema" ]);

	var properties = schema.properties;
	var ans = true;

	for (var i = 0; i < columns.size; i++) {
		if (typeof values[i] !== properties[columns[i]].type)
			ans = false;
	}
	for (key in properties) {
		if (properties[key].notNull && columns.indexOf(key) !== -1)
			ans = false;
	}
	callback(undefined, ans);

};

// TODO Create Utilities file
var isSatisfingSchema = function(columns, schema, callback) {
	schemaFunctions.isSatisfingSchema(columns, schema, callback);
};

var getPrimaryKeyNameForSchema = function(namespace, callback) {
	// TODO Get primary key from schema
	// callback(new Error("No key"), undefined)
	callback(undefined, "CustomerName");
};

module.exports.DataInsertSQLPlanRunner = DataInsertSQLPlanRunner;