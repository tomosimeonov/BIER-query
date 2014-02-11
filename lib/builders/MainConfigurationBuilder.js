/**
 * New node file
 */
var dataInsertSQLPlanRunnerBuilder = require('../sqlplanrunners/DataInsertSQLPlanRunner');
var schemaInsertSQLPlanRunnerBuilder = require('../sqlplanrunners/SchemaInsertSQLPlanRunner');
var simpleSelectSQLPlanRunner = require('../sqlplanrunners/SimpleSelectSQLPlanRunner');
var joinSelectSQLPlanRunner = require('../sqlplanrunners/JoinSelectSQLPlanRunner');

createDataInsertRunner = function(storageApis,eventHolder){
	
	var store = function(namespace, key, values, keepAlive,callback){
		storageApis.Node.put(namespace, key, values, keepAlive,callback);
	};

	return new dataInsertSQLPlanRunnerBuilder.DataInsertSQLPlanRunner(store,eventHolder);
};

createSchemaInsertRunner = function(storageApis,eventHolder){
	
	var store = function(namespace, key, values, keepAlive,callback){
		//TODO
	};

	return new schemaInsertSQLPlanRunnerBuilder.SchemaInsertSQLPlanRunner(store,eventHolder);
};

createSimpleSelectSQLPlanRunner = function(simpleExecutor,eventHolder){
	return new simpleSelectSQLPlanRunner.SimpleSelectSQLPlanRunner(simpleExecutor,eventHolder);
};

createJoinSelectSQLPlanRunner = function(joinExecutor,eventHolder){
	return new joinSelectSQLPlanRunner.JoinSelectSQLPlanRunner(joinExecutor,eventHolder);
};

module.exports.createDataInsertRunner = createDataInsertRunner;
module.exports.createSchemaInsertRunner = createSchemaInsertRunner;
module.exports.createSimpleSelectSQLPlanRunner = createSimpleSelectSQLPlanRunner;
module.exports.createJoinSelectSQLPlanRunner = createJoinSelectSQLPlanRunner;