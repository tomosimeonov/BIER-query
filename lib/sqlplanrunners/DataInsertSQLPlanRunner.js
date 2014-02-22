/**
 * Runner to process SQL plans for insert operations.
 * 
 * @author Tomo Simeonov
 */
var schemeFunctions = require('../SchemeManipulationFuncs');
var utilities = require('../EmitterUtilities');
var constants = require('../Constants');

function DataInsertSQLPlanRunner(store, getScheme, eventHolder) {
	var self = this instanceof DataInsertSQLPlanRunner ? this : Object.create(DataInsertSQLPlanRunner.prototype);
	self.store = store;
	self.eventHolder = eventHolder;
	self.getScheme = getScheme;
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
	this.getScheme(namespace, function(scheme) {
		if (scheme === undefined || scheme.primaryKey === "") {
			utilities.emitError(emiter, new Error('Problem finding scheme, try later'));
		} else {
			var keyName = scheme.primaryKey;
			that.decodeInsertStatementPlan(scheme, sqlPlan, keyName, function(err, decodedData) {

				if (err) {
					utilities.emitError(emiter, err);
				} else {

					isSatisfingScheme(decodedData.columns, scheme, function(err, schemeComplied) {
						if (schemeComplied) {
							that.areValuesSatisfingScheme(decodedData.columns, decodedData.values, scheme, function(
									err, valuesSatisfy) {
								if (valuesSatisfy) {
									that.insertData(decodedData.table, decodedData.key, decodedData.columns,
											decodedData.values, decodedData.keepAliveValue, emiter, queryId);
								} else {
									utilities.emitError(emiter, err);
								}
							}, queryId);

						} else {
							utilities.emitError(emiter, new Error("Used columns not in the scheme"));
						}
					});

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
DataInsertSQLPlanRunner.prototype.decodeInsertStatementPlan = function(scheme, insertPlan, keyName, callback, queryId) {
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
				switch (scheme.properties[data.columns[i]].typ) {
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
 * Checks if values inserted satisfy the scheme.
 * 
 * @param columns
 * @param values
 * @param scheme
 * @param callback
 * @param queryId
 *            For logging tracking
 */
DataInsertSQLPlanRunner.prototype.areValuesSatisfingScheme = function(columns, values, scheme, callback, queryId) {

	function isInt(n) {
		// n = parseInt(n);
		return n != "" && !isNaN(n) && Math.round(n) == n;
	}
	function isFloat(n) {
		return n != "" && !isNaN(n) && Math.round(n) != n;
	}

	// Does not end the function
	function notSatisfying(key, callback) {
		callback(new Error("Property " + key + " does not satisfy scheme."), false);
	}

	this.eventHolder.emitEvent("LOG_FINE", [ queryId, "Checking if values satisfies scheme." ]);
	this.eventHolder.emitEvent("LOG_FINER", [ queryId,
			"Checking " + values.length + " values if are satisfying scheme." ]);

	var ans = true;

	// Checks types
	for (var i = 0; i < columns.length; i++) {
		switch (scheme.properties[columns[i]].type) {
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
			if (typeof values[columns[i]] !== scheme.properties[columns[i]].type) {
				ans = false;
				notSatisfying(columns[i], callback);
				continue;
			}
			break;
		}
	}
	if (ans) {

		// Checks for not null, have to be in different loop
		for (key in scheme.properties) {
			if (scheme.properties[key].notNull && columns.indexOf(key) === -1) {
				ans = false;
				callback(new Error("Property " + key + " is missing."), ans);
				continue;
			}
		}
		if (ans)
			callback(undefined, ans);
	}

};

// TODO Create Utilities file
var isSatisfingScheme = function(columns, scheme, callback) {
	schemeFunctions.isSatisfingScheme(columns, scheme, callback);
};

module.exports.DataInsertSQLPlanRunner = DataInsertSQLPlanRunner;