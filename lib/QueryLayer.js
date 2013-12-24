/**
 * The main entry point for the query layer
 */

// Project imports
var parser = require('./bierSqlParser');
var queryPlanProcessor = require('./QueryPlanProcessor');
var schemaFunctions = require('./SchemaManipulationFuncs');
var insertFunctions = require('./InsertManipulationFuncs');
var indexRunnerBuilder = require('./IndexSearch');
var phtWrapperBuilder = require('./PHTWrapper');
var joinExecutorBuilder = require('./executors/JoinExecutor');
var simpleExecutorBuilder = require('./executors/SimpleExecutor');
var statisticHolder = require('./StatisticHolder');
var joinBuilderFactory = require('./builders/JoinQueryConfigurationBuilder');
var simpleBuilderFactory = require('./builders/SimpleQueryConfigurationBuilder');

// 3rd party imports
var curry = require('curry');

function QueryLayer(storageApis) {
	var self = this instanceof QueryLayer ? this : Object.create(QueryLayer.prototype);
	self.storageApis = storageApis;
	self.statisticHolder = new statisticHolder.StatisticHolder;
	self.simpleExecutor = simpleExecutorBuilder.SimpleExecutor(storageApis, self.statisticHolder);
	self.joinExecutor = joinExecutorBuilder.JoinExecutor(storageApis, self.statisticHolder);
	self.storageApis.Node.registerMessageHandler(_router({
		'simple' : self.simpleExecutor
	}));
	return self;
}

var _router = curry(function(routees, notParsemessage) {
	var message = JSON.parse(notParsemessage);
	if (message.type && routees[message.type]) {
		routees[message.type].message(message);
	}
});

QueryLayer.prototype.executeSQL = function(sql, callback) {
	var that = this;
	var simpleSelectCheck = function(data) {
		return data.SELECT !== undefined && !joinCheck(data);
	};

	var joinCheck = function(data) {
		return data.SELECT !== undefined && data['INNER JOIN'] || data['LEFT JOIN'] || data['RIGHT JOIN'];
	};

	parser(sql, function(err, data) {
		if (err) {
			callback(err, null);
		} else {
			if (data.table) {
				that.createSchema(data.table, data, callback);
			} else if (data.createIndex) {
				that.createIndex(data.createIndex.table, data.createIndex.property, callback);
			} else if (data['INSERT INTO']) {

				that.getPrimaryKeyNameForSchema(data['INSERT INTO'].table, function(err, keyName) {
					if (err || keyName === undefined || keyName === "")
						callback(new Error('Problem finding primary key try later'), undefined);
					else
						insertFunctions.decodeInsertStatementPlan(data, keyName, function(err, decodedData) {
							if (err) {
								callback(err, undefined);
							} else {
								that.insertData(decodedData.table, decodedData.key, decodedData.columns,
										decodedData.values, decodedData.keepAliveValue, callback);
							}
						});
				});
			} else if (simpleSelectCheck(data)) {
				that.selectQuery(data.SELECT, data.FROM[0], data.WHERE, callback);
			} else if (joinCheck(data)) {
				that.joinQuery(data, callback);
			} else
				callback(new Error('Not recognized syntaxis.'), undefined);
		}

	});
};

QueryLayer.prototype.createSchema = function(namespace, schema, callback) {
	schemaFunctions.permenentSchemaCreateror(schema, function(err, realSchema) {
		if (err)
			callback(err, undefined);
		else
			schemaFunctions.isSatisfingSchemaStructure(realSchema, function(err, correctness) {
				if (err)
					callback(err, undefined);
				else {
					// TODO schema in DHT
					callback(undefined, "Schema for namespace " + namespace + " is been inserted in the database.");
				}
			});
	});
};

QueryLayer.prototype.createIndex = function(namespace, property, callback) {
	// TODO create index

	callback("Have to create index for " + property + " from namespace " + namespace);
};

QueryLayer.prototype.insertData = function(namespace, key, columns, values, keepAlive, callback) {
	var that = this;
	if (key === undefined || keepAlive === undefined)
		callback(new Error("No primary key value or keep alive provided"), undefined);
	else {
		// TODO get schema and properties correctly
		var schema = columns;
		var schemaPropNames = columns;

		schemaFunctions.isSatisfingSchema(columns, schema, function(err, schemaComplied) {
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

				that.storageApis.Node.put(namespace, key, values, keepAlive, callback);
			} else
				callback(new Error("Used columns not in the schema"), undefined);
		});
	}
};

QueryLayer.prototype.selectQuery = function(properties, namespace, filterPlan, callback) {
	var that = this;

	// TODO Extract Schema
	// TODO Validate
	// TODO Index Search
	// TODO Extract correctly timeout
	this.simpleExecutor.formatSelectProperties(properties, function(err, formatedSelectProperties) {
		//var newThat = that;

		// TODO extract from PHT
		var destinations = [];

		var builder = simpleBuilderFactory.SimpleQueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations(destinations).setFilterPlan(filterPlan)
				.setFormattedProperties(formatedSelectProperties).setTimeout(10);

		that.simpleExecutor.registerNewQuery(builder.buildQueryConfig(), callback, function(err, id) {
			that.simpleExecutor.executeQuery(id);
		});
	});

};

QueryLayer.prototype.joinQuery = function(plan, callback) {
	var that = this;
	_createJoinConfiguration(plan, this, function(err, config) {
		if (err) {
			callback(err, undefined);
		} else {
			that.joinExecutor.registerNewQuery(config, callback, function(err, id) {
				that.joinExecutor.executeQuery(id);
			});
		}

	});

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

// Utility functions
var _createJoinConfiguration = function(plan, context, callback) {
	var builder = joinBuilderFactory.JoinQueryConfigurationBuilder();

	var join = plan['LEFT JOIN'];
	var type = builder.ONE;

	if (join === undefined) {
		join = plan['INNER JOIN'];
		type = builder.NONE;
	}
	if (join === undefined) {
		join = plan['RIGHT JOIN'];
		type = builder.TWO;
	}

	if (join === undefined || join.cond.operator === undefined || join.cond.operator !== "=")
		callback(new Error("Not recognized join syntaxis"), undefined);
	else {
		var left = join.cond.left.split('.');
		var right = join.cond.right.split('.');

		builder = builder.setNamespaceOne(plan.FROM[0]).setNamespaceTwo(join.table).setFullType(type);
		if (left[0] === plan.FROM[0]) {
			builder = builder.setJoinPropertyOne(left[1]).setJoinPropertyTwo(right[1]);
		} else {
			builder = builder.setJoinPropertyOne(right[1]).setJoinPropertyTwo(left[1]);
		}
		// TODO make the time extraction correct
		builder = builder.setFilterPlan(plan.WHERE).setTimeout(30000);

		context.joinExecutor.formatSelectProperties(plan.SELECT, function(err, props) {
			if (err) {
				callback(err, undefined);
			} else {
				callback(undefined, builder.setFormattedProperties(props).buildQueryConfig());
			}

		});
	}

};

module.exports.QueryLayer = QueryLayer;