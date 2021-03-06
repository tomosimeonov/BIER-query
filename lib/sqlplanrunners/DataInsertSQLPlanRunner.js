/**
 * Runner to process SQL plans for insert operations.
 * 
 * @author Tomo Simeonov
 */
var utilities = require('../EmitterUtilities');
var constants = require('../Constants');

function DataInsertSQLPlanRunner(store, getSchema, eventHolder) {
	var self = this instanceof DataInsertSQLPlanRunner ? this : Object.create(DataInsertSQLPlanRunner.prototype);
	self.store = store;
	self.eventHolder = eventHolder;
	self.getSchema = getSchema;
	self.counter = 0;
	return self;
};

/**
 * Main entry point for executing sql insert data plan.
 * 
 * @param sqlPlan
 *            Insert Data Statement
 * @param emiter
 */
DataInsertSQLPlanRunner.prototype.executePlan = function(sqlPlan, emiter) {

	var that = this;

	var queryId = "insert-" + this.counter;
	this.counter++;

	utilities.emitExecuting(emiter, queryId);
	this.eventHolder.emitEvent(constants.LOG_INFO, [ queryId, "Inserting data into the database" ]);

	var namespace = sqlPlan['INSERT INTO'].table;
	this.getSchema(namespace, function(schema) {
		if (schema === undefined || schema.primaryKey === "") {
			utilities.emitError(emiter, new Error('Problem finding schema, try later'));
		} else {
			var keyName = schema.primaryKey;
			that.decodeInsertStatementPlan(schema, sqlPlan, keyName, function(err, decodedData) {

				if (err) {
					utilities.emitError(emiter, err);
				} else {
					var data = {};
					data[namespace] = decodedData.columns;
					var schemas = {};
					schemas[namespace] = schema;

					that.areValuesSatisfingSchema(decodedData.columns, decodedData.values, schema, function(err,
							valuesSatisfy) {
						if (valuesSatisfy) {
							that.insertData(decodedData.table, decodedData.key, decodedData.columns,
									decodedData.values, decodedData.keepAliveValue, emiter, queryId);
						} else {
							utilities.emitError(emiter, err);
						}
					}, queryId);

				}
			}, queryId);
		}
	});
};

/**
 * Inserts data in the storage layer
 * 
 * @param namespace
 * @param key
 * @param columns
 * @param values
 * @param keepAlive
 * @param emiter
 * @param queryId
 *            For logging tracking
 */
DataInsertSQLPlanRunner.prototype.insertData = function(namespace, key, columns, values, keepAlive, emiter, queryId) {
	var that = this;
	this.eventHolder.emitEvent("LOG_FINER", [ queryId, "Sending the new data to the storage layer." ]);
	this.store(namespace, key, values, keepAlive, function(data) {
		that.eventHolder.emitEvent("LOG_FINER", [ queryId, "Storage layer finished execution." ]);
		if (data == null) {
			utilities.emitError(emiter, new Error("Storage layer returned error."));
		} else {
			utilities.emitSuccess(emiter, data);
		}
		utilities.emitQueryFinish(emiter);
	});

};

// TODO Move unmarshaller here, it is the place that makes the problem
/**
 * Decodes the SQL insert statement into the internal format.
 * 
 * @param insertPlan
 * @param keyName
 *            Primary key name
 * @param callback
 * @param queryId
 *            For logging tracking
 */
DataInsertSQLPlanRunner.prototype.decodeInsertStatementPlan = function(schema, insertPlan, keyName, callback, queryId) {
	this.eventHolder.emitEvent("LOG_FINE", [ queryId, "Transforming sql data to internal structure" ]);

	function isInt(n) {
		return n != "" && !isNaN(n) && Math.round(n) == n;
	}

	var data = {};
	data.table = insertPlan['INSERT INTO'].table;
	data.columns = insertPlan['INSERT INTO'].columns;
	data.values = insertPlan.VALUES[0];

	if (data.columns.indexOf(keyName) == -1)
		callback(new Error('No primary key provided.'), undefined);
	else {
		data.key = data.values[data.columns.indexOf(keyName)];
		var keepAliveIndex = data.columns.indexOf('KEEP_ALIVE');
		if (keepAliveIndex > -1 && isInt(data.values[keepAliveIndex])) {
			data.keepAliveValue = parseInt(data.values[keepAliveIndex]);
			data.columns.splice(keepAliveIndex, 1);
			data.values.splice(keepAliveIndex, 1);

			var tempvalues = {};
			for (var i = 0; i < data.columns.length; i++) {
				switch (schema.properties[data.columns[i]].typ) {
				case 'int':
					tempvalues[data.columns[i]] = parseInt(data.values[i]);
					break;
				case 'float':
					tempvalues[data.columns[i]] = parseFloat(data.values[i]);
					break;
				default:
					tempvalues[data.columns[i]] = data.values[i];
					break;
				}

			}
			data.values = tempvalues;
			callback(undefined, data);
		} else {
			callback(new Error('No KEEP_ALIVE'), undefined);
		}
	}
};

/**
 * Checks if values inserted satisfy the schema.
 * 
 * @param columns
 * @param values
 * @param schema
 * @param callback
 * @param queryId
 *            For logging tracking
 */
DataInsertSQLPlanRunner.prototype.areValuesSatisfingSchema = function(columns, values, schema, callback, queryId) {

	function isInt(n) {
		// n = parseInt(n);
		return n != "" && !isNaN(n) && Math.round(n) == n;
	}
	function isFloat(n) {
		return n != "" && !isNaN(n);
	}

	// Does not end the function
	function notSatisfying(key, callback) {
		callback(new Error("Property " + key + " does not satisfy schema."), false);
	}

	this.eventHolder.emitEvent("LOG_FINE", [ queryId, "Checking if values satisfies schema." ]);
	this.eventHolder.emitEvent("LOG_FINER", [ queryId,
			"Checking " + values.length + " values if are satisfying schema." ]);

	var ans = true;

	// Checks types
	for (var i = 0; i < columns.length; i++) {
		switch (schema.properties[columns[i]].type) {
		case "int":
			if (!isInt(values[columns[i]])) {
				ans = false;
				notSatisfying(columns[i], callback);
				continue;
			}

			break;
		case "float":
			if (!isFloat(values[columns[i]])) {
				ans = false;
				notSatisfying(columns[i], callback);
				continue;
			}
			break;
		default:
			if (typeof values[columns[i]] !== schema.properties[columns[i]].type) {
				ans = false;
				notSatisfying(columns[i], callback);
				continue;
			}
			break;
		}
	}
	if (ans) {

		// Checks for not null, have to be in different loop
		for (key in schema.properties) {
			if (schema.properties[key].notNull && columns.indexOf(key) === -1) {
				ans = false;
				callback(new Error("Property " + key + " is missing."), ans);
				continue;
			}
		}
		if (ans)
			callback(undefined, ans);
	}

};

module.exports.DataInsertSQLPlanRunner = DataInsertSQLPlanRunner;