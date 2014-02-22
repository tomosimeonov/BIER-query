/**
 * New node file
 */
var dataInsertSQLPlanRunnerBuilder = require('../sqlplanrunners/DataInsertSQLPlanRunner');
var schemeInsertSQLPlanRunnerBuilder = require('../sqlplanrunners/SchemeInsertSQLPlanRunner');
var simpleSelectSQLPlanRunner = require('../sqlplanrunners/SimpleSelectSQLPlanRunner');
var joinSelectSQLPlanRunner = require('../sqlplanrunners/JoinSelectSQLPlanRunner');

createDataInsertRunner = function(storageApis,eventHolder){
	
	var store = function(namespace, key, values, keepAlive,callback){
		storageApis.Node.put(namespace, key, values, keepAlive,callback);
	};

	var getScheme = function(namespace,callback){
		storageApis.Node.getGlobal(namespace,callback);
	};
	
	return new dataInsertSQLPlanRunnerBuilder.DataInsertSQLPlanRunner(store,getScheme,eventHolder);
};

createSchemeInsertRunner = function(storageApis,eventHolder){
	
	var store = function(namespace,value){
		storageApis.Node.setGlobal(namespace,value);
	};

	return new schemeInsertSQLPlanRunnerBuilder.SchemeInsertSQLPlanRunner(store,eventHolder);
};

createSimpleSelectSQLPlanRunner = function(simpleExecutor,eventHolder){
	return new simpleSelectSQLPlanRunner.SimpleSelectSQLPlanRunner(simpleExecutor,eventHolder);
};

createJoinSelectSQLPlanRunner = function(joinExecutor,eventHolder){
	return new joinSelectSQLPlanRunner.JoinSelectSQLPlanRunner(joinExecutor,eventHolder);
};

module.exports.createDataInsertRunner = createDataInsertRunner;
module.exports.createSchemeInsertRunner = createSchemeInsertRunner;
module.exports.createSimpleSelectSQLPlanRunner = createSimpleSelectSQLPlanRunner;
module.exports.createJoinSelectSQLPlanRunner = createJoinSelectSQLPlanRunner;