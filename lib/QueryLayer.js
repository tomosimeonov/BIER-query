/**
 * The main entry point for the query layer
 */

// Project imports
var parser = require('./bierSqlParser');
var queryPlanProcessor = require('./QueryPlanProcessor');
var eventHolder = require('./StatisticHolder');
var joinExecutorBuilder = require('./executors/JoinExecutor');
var simpleExecutorBuilder = require('./executors/SimpleExecutor');
var mainConfiguration = require('./builders/MainConfigurationBuilder');
var emitterUtilities = require('./EmitterUtilities');
//var indexRunnerBuilder = require('./IndexSearch');
//var phtWrapperBuilder = require('./PHTWrapper');

// 3rd party imports
var curry = require('curry');

function QueryLayer(storageApis) {
	var self = this instanceof QueryLayer ? this : Object.create(QueryLayer.prototype);
	self.storageApis = storageApis;
	self.eventHolder = new eventHolder.StatisticHolder;
	self.simpleExecutor = simpleExecutorBuilder.SimpleExecutor(storageApis, self.eventHolder);
	self.joinExecutor = joinExecutorBuilder.JoinExecutor(storageApis, self.eventHolder);
	self.storageApis.Node.registerMessageHandler(_router({
		'simple' : self.simpleExecutor,
		'join' : self.joinExecutor
	}));
	
	self.dataInsertSQLPlanRunner = mainConfiguration.createDataInsertRunner(storageApis,self.eventHolder);
	self.schemaInsertSQLPlanRunner = mainConfiguration.createSchemeInsertRunner(storageApis,self.eventHolder);
	self.simpleSelectSQLPlanRunner = mainConfiguration.createSimpleSelectSQLPlanRunner(self.simpleExecutor,self.eventHolder);
	self.joinSelectSQLPlanRunner = mainConfiguration.createJoinSelectSQLPlanRunner(self.joinExecutor,self.eventHolder);
	return self;
}

var _router = curry(function(routees, notParsedMessage) {
	var message = JSON.parse(notParsedMessage);
	if (message.type && routees[message.type]) {
		routees[message.type].message(message);
	}
});

QueryLayer.prototype.executeSQL = function(sql, emiter) {
	var that = this;
	var isSimpleSelect = function(data) {
		return data.SELECT !== undefined && !isJoinSelect(data);
	};

	var isJoinSelect = function(data) {
		return data.SELECT !== undefined && data['INNER JOIN'] || data['LEFT JOIN'] || data['RIGHT JOIN'];
	};
	
	var isInsert = function(sql){
		return sql['INSERT INTO'] !== undefined;
	};

	parser(sql, function(err, sqlPlan) {
		if (err) {
			emitterUtilities.emitError(emiter, err);
		} else {
			if (sqlPlan.table) {
				that.schemaInsertSQLPlanRunner.executePlan(sqlPlan, emiter);
			} else if (sqlPlan.createIndex) {
				that.createIndex(sqlPlan.createIndex.table, sqlPlan.createIndex.property, emiter);
			} else if (isInsert(sqlPlan)) {
				that.dataInsertSQLPlanRunner.executePlan(sqlPlan,emiter);
			} else if (isSimpleSelect(sqlPlan)) {
				that.simpleSelectSQLPlanRunner.executePlan(sqlPlan,emiter);
			} else if (isJoinSelect(sqlPlan)) {
				that.joinSelectSQLPlanRunner.executePlan(sqlPlan, emiter);
			} else
				emitterUtilities.emitError(emiter, new Error('Not recognized syntaxis.'));
		}

	});
};

QueryLayer.prototype.createIndex = function(namespace, property, callback) {
	// TODO create index
	emitterUtilities.emitSuccess(emiter, "Have to create index for " + property + " from namespace " + namespace);

};

QueryLayer.prototype.checkQueryPlanAgainstSchema = function(schema, queryPlan, callback) {
	queryPlanProcessor.extractAllProperties(queryPlan, function(data) {
		queryPlanProcessor.isSatisfingSchema(data, schema, callback);
	});
};

QueryLayer.prototype.getPrimaryKeyNameForSchema = function(namespace, callback) {
	// TODO Get primary key from schema
	// callback(new Error("No key"), undefined)
	callback(undefined, "CustomerName");

};

module.exports.QueryLayer = QueryLayer;