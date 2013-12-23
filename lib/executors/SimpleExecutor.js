/**
 * Simple Query Executor for simple type of queries.
 */

//Local imports
var filterBuilder = require('../FilterBuilder');
var messageBuilderBuilder = require('../QueryMessageBuilder');
var aggregationHelper = require('../AggregationHelper').AggregationHelper();

//3d Party
var curry = require('curry');

//Internal tags 
var STORE = 0;
var DISCARD = 1;

function SimpleExecutor(storageApis, statisticHolder) {
	var self = this instanceof SimpleExecutor ? this : Object.create(SimpleExecutor.prototype);
	self.storageApis = storageApis;
	self.statisticHolder = statisticHolder;

	self.runningQueries = [];
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
 * Handle to process received messages.
 * @param message Message to process.
 */
SimpleExecutor.prototype.message = function(message) {
	var queryId = message.queryId;
	if (message.origin === this.nodeId) {
		if (this.runningQueries[queryId] !== undefined && this.runningQueries[queryId].state === STORE) {
			this.updateQueryHolderInformation(queryId, message.payload);
		} else {
			// Already sent to the client, do nothing
		}
	} else {
		if (this.runningQueries[queryId] === undefined) {
			if (message.payloadType === "DATA") {
				this.storageApis.send(message.origin, message, function() {
				});
			} else if (message.payloadType === "CONFIG") {
				this.executeNonRegQueryOnRemoteCall(message.queryId, message.payload, message.origin);
			}
		} else {
			if (this.runningQueries[queryId].state === STORE && message.payloadType === "DATA") {
				this.updateQueryHolderInformation(queryId, message.payload);
			} else if (this.runningQueries[queryId].state === DISCARD && message.payloadType === "DATA") {
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
 * @param queryId The id of the query to be send.
 */
SimpleExecutor.prototype.distribute = function(queryId) {
	var that = this;
	var message = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setSimpleType()
			.setOrigin(this.nodeId).setPayloadTypeConfig().setPayload(this.runningQueries[queryId].configuration)
			.buildMessage();
	if (this.runningQueries[queryId].configuration.destinations.length === 0) {
		this.storageApis.Node.broadcast(JSON.stringify(message));
	} else {
		// TODO Do for id to id
		this.runningQueries[queryId].configuration.destinations.forEach(function(dest) {
			that.storageApis.send(dest, message, function() {
			});
		});
	}
};

/**
 * Registers and executes new remote query.
 * @param queryId The queryId to be registered. 
 * @param configuration The query configuration.
 * @param origin The origin of the query.
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

	this.runningQueries[queryId] = _buildMemory(configuration, callSender, STORE);
	this.runningQueries[queryId].configuration.timeoutValue = _setTimeouts(queryId, callSender, -1,
			configuration.timeoutValue / 5, this);
	this.updateWithLocalMatch(queryId);
	this.statisticHolder.updateStatistics(this.statisticHolder.NEW_QUERY);
};

/**
 * Registers new query to be executed in the network with this node the origin.
 * @param configuration The configuration to be used for execution.
 * @param queryCallback The callback to return data at the end.
 * @param idCallback The callback to return id of the query.
 */
SimpleExecutor.prototype.registerNewQuery = function(configuration, queryCallback, idCallback) {
	this.next = this.next + 1;
	var queryId = this.nodeId + "-" + this.next;

	this.runningQueries[queryId] = _buildMemory(configuration, queryCallback, STORE);

	if (configuration.timeout !== undefined) {
		configuration.timeout = configuration.timeout * 1000;
	}

	this.runningQueries[queryId].configuration.timeoutValue = _setTimeouts(queryId, queryCallback,
			configuration.maxObjects, configuration.timeout, this);

	this.statisticHolder.updateStatistics(this.statisticHolder.NEW_QUERY);
	idCallback(undefined, queryId);
};

/**
 * Helper method to update information in the local cache.
 * @param queryId The query id.
 * @param data Data to be return to the client. 
 */
SimpleExecutor.prototype.updateQueryHolderInformation = function(queryId, data) {
	if (this.runningQueries[queryId] !== undefined) {
		this.runningQueries[queryId].data = this.runningQueries[queryId].data.concat(data);
		this.runningQueries[queryId].currentNumObjects = this.runningQueries[queryId].currentNumObjects + data.length;
		if (this.runningQueries[queryId].currentNumObjects === this.runningQueries[queryId].configuration.maxObjects) {
			// Have to terminate
			this.returnInformation(queryId, this.runningQueries[queryId].queryCallback);
		} else if (this.runningQueries[queryId].configuration.maxObjects !== -1) {
			// Restart timer for object count
			_objectCountStopper(queryId, this.runningQueries[queryId].currentNumObjects,
					this.runningQueries[queryId].queryCallback, this);
		}
	}
};

/**
 * Helper method to filter for local matches.
 * @param queryId The query id.
 */
SimpleExecutor.prototype.updateWithLocalMatch = function(queryId) {
	var that = this;
	var namespace = this.runningQueries[queryId].configuration.namespace;
	var properties = this.runningQueries[queryId].configuration.properties;
	var filterPlan = this.runningQueries[queryId].configuration.filterPlan;

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
 * @param queryId The query id.
 * @param callback The callback to return to.
 */
SimpleExecutor.prototype.returnInformation = function(queryId, callback) {
	if (this.runningQueries[queryId]) {
		var queryData = this.runningQueries[queryId];
		this.runningQueries[queryId].state = DISCARD;
		_removeQueryFromMemory(queryId, this.runningQueries[queryId].timeoutValue, this);
		var data = queryData.data;
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
 * Helper method to process select properties to format acceptable by the executor.
 * @param selectProperties The properties in the select.
 * @param callback The callback to return them.
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

var _setTimeouts = function(queryId, queryCallback, maxObjects, timeout, context) {
	// Set up object count timeout if any
	if (maxObjects !== -1) {
		_objectCountStopper(queryId, 0, queryCallback, context);
		return context.defaultObjectTimeoutInMillis;
	}

	// Set up timeout if any
	if (timeout !== undefined) {
		setTimeout(function() {
			context.returnInformation(queryId, queryCallback);
		}, timeout);
		return timeout;
	}

	// Default timeout if non provided
	if (maxObjects === -1 && timeout === undefined) {
		setTimeout(function() {
			context.returnInformation(queryId, queryCallback);
		}, context.defaultTimeoutInMillis);
		return context.defaultTimeoutInMillis;
	}
};

var _objectCountStopper = function(queryId, prevObjCount, queryCallback, context) {
	setTimeout(function() {
		if (context.runningQueries[queryId] !== undefined
				&& context.runningQueries[queryId].currentNumObjects === prevObjCount)
			context.returnInformation(queryId, queryCallback);
	}, context.defaultObjectTimeoutInMillis);
};

var _removeQueryFromMemory = function(queryId, timeout, context) {
	setTimeout(function() {
		if (context.runningQueries[queryId]) {
			delete context.runningQueries[queryId];
		}
	}, context.runningQueries[queryId].configuration.timeoutValue * 5);
};

module.exports.SimpleExecutor = SimpleExecutor;