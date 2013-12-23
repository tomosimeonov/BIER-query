/**
 * Query Executor for simple type of queries.
 */
var curry = require('curry');
var filterBuilder = require('../FilterBuilder');
var aggregationHelper = require('../AggregationHelper').AggregationHelper();
var messageBuilderBuilder = require('../QueryMessageBuilder');

var STORE = 0;
var DISCARD = 1;

function JoinExecutor(storageApis, statisticHolder) {
	var self = this instanceof JoinExecutor ? this : Object.create(JoinExecutor.prototype);
	self.storageApis = storageApis;
	self.statisticHolder = statisticHolder;

	self.runningQueries = [];
	self.defaultObjectTimeoutInMillis = 8000;

	// Default time timeout if no type of timeout provided
	self.defaultTimeoutInMillis = 20000;

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
JoinExecutor.prototype.executeQuery = function(queryId) {
	this.runningQueries[queryId].configuration.timeoutValue = _setTimeouts(queryId,
			this.runningQueries[queryId].queryCallback, this.runningQueries[queryId].configuration.maxObjects,
			this.runningQueries[queryId].configuration.timeout, this);
	this.distribute(queryId);
	this.updateWithLocalMatch(queryId);
};

/**
 * Handle to process received messages.
 * 
 * @param message
 *            Message to process
 */
JoinExecutor.prototype.message = function(message) {
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
 * 
 * @param queryId
 *            The id of the query to be send.
 */
JoinExecutor.prototype.distribute = function(queryId) {
	var message = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setJoinType().setOrigin(this.nodeId)
			.setPayloadTypeConfig().setPayload(this.runningQueries[queryId].configuration).buildMessage();
	this.storageApis.Node.broadcast(JSON.stringify(message));
};

// TODO Make for joins
/**
 * Registers and executes new remote query
 * 
 * @param queryId
 *            The queryId to be registered
 * @param configuration
 *            The query configuration
 * @param origin
 *            The origin of the query.
 */
JoinExecutor.prototype.executeNonRegQueryOnRemoteCall = function(queryId, configuration, origin) {
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

// TODO Make for joins
JoinExecutor.prototype.registerNewQuery = function(configuration, queryCallback, idCallback) {
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

// TODO Make for joins
JoinExecutor.prototype.updateQueryHolderInformation = function(queryId, data) {
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

// TODO Make for joins
JoinExecutor.prototype.updateWithLocalMatch = function(queryId) {
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

// TODO Make for joins
JoinExecutor.prototype.returnInformation = function(queryId, callback) {
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

// TODO Make for joins
JoinExecutor.prototype.formatSelectProperties = function(selectProperties, callback) {
	// aggregationHelper.propertiesProcessor(selectProperties, callback);

	var data = {};
	if (selectProperties[0] === '*') {
		callback(undefined, data);
	} else {
		var tempTransformed = selectProperties.map(function(elem) {
			var el = elem.split('.');
			if (el.length !== 2 || el.indexOf('(') !== -1) {
				return {};
			} else {
				var data = {};
				data[el[0]] = el[1];
				return data;
			}
		});
		var size = 0;
		data = tempTransformed.reduce(function(prev, curr) {
			for (prop in curr) {
				if (prev[prop] !== undefined) {
					prev[prop].push(curr[prop]);
				} else {
					prev[prop] = [ curr[prop] ];
					size = size + 1;
				}
			}
			return prev;
		}, {});
		if (size === 2) {
			callback(undefined, data);
		} else {
			callback(new Error("Not recognized syntaxis in select."), undefined);
		}
	}
};

// Utility methods
// TODO Make for joins
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

module.exports.JoinExecutor = JoinExecutor;