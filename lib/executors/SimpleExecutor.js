/**
 * Simple Query Executor for simple type of queries.
 */

// Local imports
var filterBuilder = require('../FilterBuilder');
var messageBuilderBuilder = require('../builders/QueryMessageBuilder');
var aggregationHelper = require('../AggregationHelper').AggregationHelper();
var utilities = require('./ExecutorUtilities');

// 3d Party
var curry = require('curry');

// Internal tags
var STORE = 0;
var DISCARD = 1;

function SimpleExecutor(storageApis, statisticHolder) {
	var self = this instanceof SimpleExecutor ? this : Object.create(SimpleExecutor.prototype);
	self.storageApis = storageApis;
	self.statisticHolder = statisticHolder;

	self.queryConfigurations = [];
	self.runningData = [];
	self.defaultObjectTimeoutInMillis = 4000;

	// Default time timeout if no type of timeout provided
	self.defaultTimeoutInMillis = 10000;

	// Hack to get the id
	self.nodeId = storageApis.Node.node.getID();

	// Helper for query ids
	self.next = -1;
	return self;
}

/**
 * Executes query.
 * 
 * @param queryId
 *            The id of the query to be executed.
 */
SimpleExecutor.prototype.executeQuery = function(configuration, queryCallback) {
	var that = this;
	this.registerNewQuery(configuration, queryCallback, function(err,queryId) {
		that.queryConfigurations[queryId].configuration.timeoutValue = utilities.setTimeouts(queryId,
				that.queryConfigurations[queryId].queryCallback,
				that.queryConfigurations[queryId].configuration.maxObjects,
				that.queryConfigurations[queryId].configuration.timeout, that);
		that.distribute(queryId);
		that.updateWithLocalMatch(queryId);
	});
};

/**
 * Handle to process received messages.
 * 
 * @param message
 *            Message to process.
 */
SimpleExecutor.prototype.message = function(message) {
	var queryId = message.queryId;
	if (message.origin === this.nodeId) {
		if (this.queryConfigurations[queryId] !== undefined && this.queryConfigurations[queryId].state === STORE) {
			this.updateQueryHolderInformation(queryId, message.payload);
		} else {
			// Already sent to the client, do nothing
		}
	} else {
		if (this.queryConfigurations[queryId] === undefined) {
			if (message.payloadType === "DATA") {
				this.storageApis.send(message.origin, message, function() {
				});
			} else if (message.payloadType === "CONFIG") {
				this.executeNonRegQueryOnRemoteCall(message.queryId, message.payload, message.origin);
			}
		} else {
			if (this.queryConfigurations[queryId].state === STORE && message.payloadType === "DATA") {
				this.updateQueryHolderInformation(queryId, message.payload);
			} else if (this.queryConfigurations[queryId].state === DISCARD && message.payloadType === "DATA") {
				this.storageApis.send(message.origin, message, function() {
				});
			} else {
				// Should stop sending it
			}
		}
	}

};

/**
 * Distributes query across the network.
 * 
 * @param queryId
 *            The id of the query to be send.
 */
SimpleExecutor.prototype.distribute = function(queryId) {
	var that = this;
	var message = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setSimpleType()
			.setOrigin(this.nodeId).setPayloadTypeConfig().setPayload(this.queryConfigurations[queryId].configuration)
			.buildMessage();
	if (this.queryConfigurations[queryId].configuration.destinations.length === 0) {
		this.storageApis.Node.broadcast(JSON.stringify(message));
	} else {
		// TODO Do for id to id
		this.queryConfigurations[queryId].configuration.destinations.forEach(function(dest) {
			that.storageApis.send(dest, message, function() {
			});
		});
	}
};

/**
 * Registers and executes new remote query.
 * 
 * @param queryId
 *            The queryId to be registered.
 * @param configuration
 *            The query configuration.
 * @param origin
 *            The origin of the query.
 */
SimpleExecutor.prototype.executeNonRegQueryOnRemoteCall = function(queryId, configuration, origin) {
	var that = this;
	var callSenderPrototype = curry(function(origin, err, data) {
		var message = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setSimpleType().setOrigin(origin)
				.setPayloadTypeData().setPayload(data).buildMessage();
		that.storageApis.Node.send(origin, JSON.stringify(message), function(data) {
		});
	});

	var callSender = callSenderPrototype(origin);

	this.runningData[queryId] = [];

	this.queryConfigurations[queryId] = _buildMemory(configuration, callSender, STORE);
	this.queryConfigurations[queryId].configuration.timeoutValue = utilities.setTimeouts(queryId, callSender, -1,
			configuration.timeoutValue / 5, this);
	this.updateWithLocalMatch(queryId);
	this.statisticHolder.updateStatistics(this.statisticHolder.NEW_QUERY);
};

/**
 * Registers new query to be executed in the network with this node the origin.
 * 
 * @param configuration
 *            The configuration to be used for execution.
 * @param queryCallback
 *            The callback to return data at the end.
 * @param idCallback
 *            The callback to return id of the query.
 */
SimpleExecutor.prototype.registerNewQuery = function(configuration, queryCallback, idCallback) {
	this.next = this.next + 1;
	var queryId = this.nodeId + "-" + this.next;

	this.queryConfigurations[queryId] = _buildMemory(configuration, queryCallback, STORE);
	this.runningData[queryId] = [];

	if (configuration.timeout !== undefined) {
		configuration.timeout = configuration.timeout * 1000;
	}

	this.statisticHolder.updateStatistics(this.statisticHolder.NEW_QUERY);
	idCallback(undefined, queryId);
};

/**
 * Helper method to update information in the local cache.
 * 
 * @param queryId
 *            The query id.
 * @param data
 *            Data to be return to the client.
 */
SimpleExecutor.prototype.updateQueryHolderInformation = function(queryId, data) {
	if (this.queryConfigurations[queryId] !== undefined) {
		this.runningData[queryId] = this.runningData[queryId].concat(data);
		this.queryConfigurations[queryId].currentNumObjects = this.queryConfigurations[queryId].currentNumObjects
				+ data.length;
		if (this.queryConfigurations[queryId].currentNumObjects === this.queryConfigurations[queryId].configuration.maxObjects) {
			// Have to terminate
			this.returnInformation(queryId, this.queryConfigurations[queryId].queryCallback);
		} else if (this.queryConfigurations[queryId].configuration.maxObjects !== -1) {
			// Restart timer for object count
			utilities.objectCountStopper(queryId, this.queryConfigurations[queryId].currentNumObjects,
					this.queryConfigurations[queryId].queryCallback, this);
		}
	}
};

/**
 * Helper method to filter for local matches.
 * 
 * @param queryId
 *            The query id.
 */
SimpleExecutor.prototype.updateWithLocalMatch = function(queryId) {
	var that = this;
	var namespace = this.queryConfigurations[queryId].configuration.namespace;
	var properties = this.queryConfigurations[queryId].configuration.properties;
	var filterPlan = this.queryConfigurations[queryId].configuration.filterPlan;

	this.storageApis.Node.lscan(namespace, function(localData) {
		filterBuilder.createFilterFromWhereClause(filterPlan, function(err, dataFilter) {
			var values = Object.keys(localData).map(function(key) {
				return localData[key];
			});
			var filteredData = values.filter(dataFilter);
			if (filteredData.length > 0) {
				var processedFilteredData = filteredData.map(function(currentData) {
					var data = [];
					if (properties.length !== 0) {
						properties.forEach(function(oneProp) {
							data[oneProp] = currentData[oneProp];
						});
					} else {
						data = currentData;
					}
					return data;
				});
				that.updateQueryHolderInformation(queryId, processedFilteredData);
			}
		});
	});

};

/**
 * Helper method to return data to the client.
 * 
 * @param queryId
 *            The query id.
 * @param callback
 *            The callback to return to.
 */
SimpleExecutor.prototype.returnInformation = function(queryId, callback) {
	if (this.queryConfigurations[queryId]) {
		var queryData = this.queryConfigurations[queryId];
		this.queryConfigurations[queryId].state = DISCARD;
		utilities.removeQueryFromMemory(queryId, this.queryConfigurations[queryId].timeoutValue, this);
		var data = this.runningData[queryId];
		if (queryData.configuration.aggregations.length !== 0 && data.length !== 0) {
			aggregationHelper.processData(data, queryData.configuration.properties,
					queryData.configuration.aggregations, callback);
		} else {
			callback(undefined, data);
		}
		this.statisticHolder.updateStatistics(this.statisticHolder.FIN_QUERY);

		var performance = (new Date()) - queryData.beginingTime;
		this.statisticHolder.updateStatistics(this.statisticHolder.QUERY_PERF, performance);
	}
};

/**
 * Helper method to process select properties to format acceptable by the
 * executor.
 * 
 * @param selectProperties
 *            The properties in the select.
 * @param callback
 *            The callback to return them.
 */
SimpleExecutor.prototype.formatSelectProperties = function(selectProperties, callback) {
	aggregationHelper.propertiesProcessor(selectProperties, callback);
};

// Utility methods
var _buildMemory = function(configuration, queryCallback, state) {
	var temp = {};
	temp.data = [];
	temp.configuration = configuration;
	temp.currentNumObjects = 0;
	temp.state = state;
	temp.queryCallback = queryCallback;
	temp.beginingTime = new Date();
	return temp;
};

module.exports.SimpleExecutor = SimpleExecutor;