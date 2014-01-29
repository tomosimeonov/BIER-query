/**
 * New node file
 */
var dataInsertSQLPlanRunnerBuilder = require('../sqlplanrunners/DataInsertSQLPlanRunner');
var schemaInsertSQLPlanRunnerBuilder = require('../sqlplanrunners/SchemaInsertSQLPlanRunner');
var simpleSelectSQLPlanRunner = require('../sqlplanrunners/SimpleSelectSQLPlanRunner');
var joinSelectSQLPlanRunner = require('../sqlplanrunners/JoinSelectSQLPlanRunner');

createDataInsertRunner = function(storageApis){
	
	var store = function(namespace, key, values, keepAlive,callback){
		storageApis.Node.put(namespace, key, values, keepAlive,callback);
	};

	return new dataInsertSQLPlanRunnerBuilder.DataInsertSQLPlanRunner(store);
};

createSchemaInsertRunner = function(storageApis){
	
	var store = function(namespace, key, values, keepAlive,callback){
		//TODO
	};

	return new schemaInsertSQLPlanRunnerBuilder.SchemaInsertSQLPlanRunner(store);
};

createSimpleSelectSQLPlanRunner = function(simpleExecutor){
	return new simpleSelectSQLPlanRunner.SimpleSelectSQLPlanRunner(simpleExecutor);
};

createJoinSelectSQLPlanRunner = function(joinExecutor){
	return new joinSelectSQLPlanRunner.JoinSelectSQLPlanRunner(joinExecutor);
};

module.exports.createDataInsertRunner = createDataInsertRunner;
module.exports.createSchemaInsertRunner = createSchemaInsertRunner;
module.exports.createSimpleSelectSQLPlanRunner = createSimpleSelectSQLPlanRunner;
module.exports.createJoinSelectSQLPlanRunner = createJoinSelectSQLPlanRunner;