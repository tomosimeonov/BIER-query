/**
 * New node file
 */
function QueryLayer(storageApis) {

}

var parser = require('./lib/bierSqlParser');
var schemaChecker = require('./lib/schemaChecker');

QueryLayer.prototype.executeSQL = function(sql, callback) {

	var simpleSelectCheck = function(data) {
		return !(aggregationCheck(data) || joinCheck(data));
	};

	var aggregationCheck = function(data) {
		return data.SELECT.reduce(function(previousValue, currentValue, index, array) {
			return previousValue || currentValue.indexOf("MAX(") > -1 || currentValue.indexOf("MIN(") > -1
					|| currentValue.indexOf("COUNT(") > -1 || currentValue.indexOf("SUM(") > -1
					|| currentValue.indexOf("AVR(");
		}, false);
	};
	
	var joinCheck = function(data) {
		return data['INNER JOIN'] || data['LEFT JOIN'] || data['RIGHT JOIN'];
	};

	parser.bierSqlParser(sql, function(err, data) {
		if (err) {
			callback(err, null);
		} else {
			if (data.table) {
				this.createSchema(data.table, data, callback);
			} else if (data.createIndex) {
				this.createIndex(data.createIndex.table, data.createIndex.property, callback);
			} else if (data['INSERT INTO']) {

			} else if (simpleSelectCheck(data)) {
				this.selectQuery(data.SELECT, data.FROM[0],data.WHERE,callback)
			} else if (aggregationCheck(data)) {
				this.aggregationQuery(data.SELECT, data.FROM[0],data.WHERE,callback)
			} else if (joinCheck(data)) {
				var tables = dataFrom.push()
				this.joinQuery(data,callback)
			} else
				callback(new Error('Not recognized syntaxis.'), undefined);
		}

	});
};

QueryLayer.prototype.createSchema = function(namespace, schema, callback) {
	// TODO create Schema
	callback("Have to create schema for " + namespace);
};

QueryLayer.prototype.createIndex = function(namespace, property, callback) {
	// TODO create index
	callback("Have to create index for " + property + " from namespace " + namespace);
};

QueryLayer.prototype.insertData = function(namespace, key, data, keepAlive, callback) {
	// TODO insert data
	callback("Have to insert data for key " + key + " from namespace " + namespace);
};

QueryLayer.prototype.checkSchema = function(schema, queryPlan, callback) {
	schemaChecker.extractAllProperties(queryPlan, function(data) {
		schemaChecker.isSchemaComplied(data, schema, callback);
	});
};

QueryLayer.prototype.selectQuery = function(properties, namespace, filter, callback) {
	//TODO
	//extract schema
	//extract properties
	//connect to index 
	// send data
	//callback
};

QueryLayer.prototype.aggregationQuery = function(properties, namespace, filter, callback) {
	callback("Have do aggregation for namespace " + namespace);
};

QueryLayer.prototype.joinQuery = function(plan, callback) {
	callback("Have do join for " + plan);
};
