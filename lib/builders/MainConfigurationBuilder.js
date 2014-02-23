/**
 * Functions to create initial configuration used by the QueryLayer object
 * 
 * @author Tomo Simeonov
 */
var dataInsertSQLPlanRunnerBuilder = require('../sqlplanrunners/DataInsertSQLPlanRunner');
var schemaInsertSQLPlanRunnerBuilder = require('../sqlplanrunners/SchemaInsertSQLPlanRunner');
var simpleSelectSQLPlanRunner = require('../sqlplanrunners/SimpleSelectSQLPlanRunner');
var joinSelectSQLPlanRunner = require('../sqlplanrunners/JoinSelectSQLPlanRunner');

createDataInsertRunner = function(storageApis,eventHolder){
	
	var store = function(namespace, key, values, keepAlive,callback){
		storageApis.Node.put(namespace, key, values, keepAlive,callback);
	};

	var getSchema = function(namespace,callback){
		storageApis.Node.getGlobal(namespace,callback);
	};
	
	return new dataInsertSQLPlanRunnerBuilder.DataInsertSQLPlanRunner(store,getSchema,eventHolder);
};

createSchemaInsertRunner = function(storageApis,eventHolder){
	
	var store = function(namespace,value){
		storageApis.Node.setGlobal(namespace,value);
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