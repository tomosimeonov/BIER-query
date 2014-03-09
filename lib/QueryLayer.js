/**
 * The main entry point for the query layer
 * 
 * @author Tomo Simeonov
 */

// Project imports
var parser = require('./SqlParser');
var queryPlanProcessor = require('./QueryPlanProcessor');
var eventHolder = require('./StatisticHolder');
var joinExecutorBuilder = require('./executors/JoinExecutor');
var simpleExecutorBuilder = require('./executors/SimpleExecutor');
var mainConfiguration = require('./builders/MainConfigurationBuilder');
var emitterUtilities = require('./EmitterUtilities');
var schemaFuncs = require('./SchemaManipulationFuncs');
var logging = require('./Logging');
var constants = require('./Constants');

// 3rd party imports
var curry = require('curry');

function QueryLayer(storageApis) {
	var self = this instanceof QueryLayer ? this : Object.create(QueryLayer.prototype);
	self.storageApis = storageApis;
	self.eventHolder = new eventHolder.StatisticHolder;

	// Actual select executors
	self.simpleExecutor = simpleExecutorBuilder.SimpleExecutor(storageApis, self.eventHolder);
	self.joinExecutor = joinExecutorBuilder.JoinExecutor(storageApis, self.eventHolder);

	// Hack to allow multiple handlers for messages
	var routees = {};
	routees[constants.SIMPLE_TYPE_MESSAGE] = self.simpleExecutor;
	routees[constants.JOIN_TYPE_MESSAGE] = self.joinExecutor;
	
	self.storageApis.Node.registerMessageHandler(_router(routees));

	// To delegate the sql plans to the plan runners
	self.dataInsertSQLPlanRunner = mainConfiguration.createDataInsertRunner(storageApis, self.eventHolder);
	self.schemaInsertSQLPlanRunner = mainConfiguration.createSchemaInsertRunner(storageApis, self.eventHolder);
	self.simpleSelectSQLPlanRunner = mainConfiguration.createSimpleSelectSQLPlanRunner(self.simpleExecutor,
			self.eventHolder);
	self.joinSelectSQLPlanRunner = mainConfiguration.createJoinSelectSQLPlanRunner(self.joinExecutor, self.eventHolder);
	return self;
}

// Hack for the message handler
var _router = curry(function(routees, notParsedMessage) {
	var message = JSON.parse(notParsedMessage);
	if (message.type && routees[message.type]) {
		routees[message.type].message(message);
	}
});

/**
 * Executes SQL statement in the database.
 * 
 * @param sql
 * @param emiter
 */
QueryLayer.prototype.executeSQL = function(sql, emiter) {
	var that = this;
	var isSimpleSelect = function(data) {
		return data.SELECT !== undefined && !isJoinSelect(data);
	};

	var isJoinSelect = function(data) {
		return data.SELECT !== undefined && data['INNER JOIN'] || data['LEFT JOIN'] || data['RIGHT JOIN'];
	};

	var isInsert = function(sql) {
		return sql['INSERT INTO'] !== undefined;
	};

	parser(sql, function(err, sqlPlan) {
		logging.logSqlParsedEvent(that.eventHolder,err,"General");
		if (err) {
			emitterUtilities.emitError(emiter, err);
		} else {
			if (sqlPlan.table) {
				that.schemaInsertSQLPlanRunner.executePlan(sqlPlan, emiter);
			} else if (sqlPlan.createIndex) {
				that.createIndex(sqlPlan.createIndex.table, sqlPlan.createIndex.property, emiter);
			} else if (isInsert(sqlPlan)) {

				checkInsertPlan(that, sqlPlan, function(err, passedIt) {
					logging.logValidationPerformedEvent(that.eventHolder,err,passedIt,"General");
					if (err) {
						emitterUtilities.emitError(emiter, err);
					} else if (passedIt === false) {
						emitterUtilities.emitError(emiter, new Error("Used properties not in schema"));
					} else {
						that.dataInsertSQLPlanRunner.executePlan(sqlPlan, emiter);
					}
				});

			} else if (isSimpleSelect(sqlPlan)) {

				that.checkSqlPlanAgainstSchema([ sqlPlan.FROM[0] ], sqlPlan, function(err, passedIt) {
					logging.logValidationPerformedEvent(that.eventHolder,err,passedIt,"General");
					if (err) {
						emitterUtilities.emitError(emiter, err);
					} else if (passedIt === false) {
						emitterUtilities.emitError(emiter, new Error("Used properties not in schema ."));
					} else {
						that.simpleSelectSQLPlanRunner.executePlan(sqlPlan, emiter);
					}
				});

			} else if (isJoinSelect(sqlPlan)) {
				// Finds type of join
				var join = sqlPlan['LEFT JOIN'];

				if (join === undefined) {
					join = sqlPlan['INNER JOIN'];
				}
				if (join === undefined) {
					join = sqlPlan['RIGHT JOIN'];
				}

				that.checkSqlPlanAgainstSchema([ sqlPlan.FROM[0], join.table ], sqlPlan, function(err, passedIt) {
					logging.logValidationPerformedEvent(that.eventHolder,err,passedIt,"General");
					if (err) {
						emitterUtilities.emitError(emiter, err);
					} else if (passedIt === false) {
						emitterUtilities.emitError(emiter, new Error("Properties not in schemas used."));
					} else {
						that.joinSelectSQLPlanRunner.executePlan(sqlPlan, emiter);
					}
				});

			} else
				emitterUtilities.emitError(emiter, new Error('Not recognized syntaxis.'));
		}

	});
};

/**
 * Method to create index in the system, TODO
 * 
 * @param namespace
 * @param property
 * @param callback
 */
QueryLayer.prototype.createIndex = function(namespace, property, callback) {
	// TODO create index
	emitterUtilities.emitSuccess(emiter, "Have to create index for " + property + " from namespace " + namespace);

};

/**
 * Method to check if properties in the select query plan satisfy schema.
 * 
 * @param schema
 * @param queryPlan
 * @param callback
 */
QueryLayer.prototype.checkSqlPlanAgainstSchema = function(namespaces, sqlPlan, callback) {
	var namespaceHolder = {};
	for (var i = 0; i < namespaces.length; i++) {
		this.storageApis.Node.getGlobal(namespaces[i], function(schema) {
			namespaceHolder[namespaces[i]] = schema;
			if (i === namespaces.length - 1) {
				queryPlanProcessor.extractAllProperties(sqlPlan, function(err, data) {
					if (err) {
						callback(err, false);
					} else {
						schemaFuncs.isSatisfingSchema(data, namespaceHolder, callback);
					}

				});
			}

		});
	}

};

var checkInsertPlan = function(context, sqlPlan, callback) {
	context.storageApis.Node.getGlobal(sqlPlan['INSERT INTO'].table, function(schema) {
		var namespaceHolder = {};
		namespaceHolder[sqlPlan['INSERT INTO'].table] = schema;
		var data = {};
		var columns = [].concat(sqlPlan['INSERT INTO'].columns);
		columns.pop()
		data[sqlPlan['INSERT INTO'].table] = columns;
		schemaFuncs.isSatisfingSchema(data, namespaceHolder, callback);
	});
};

module.exports.QueryLayer = QueryLayer;