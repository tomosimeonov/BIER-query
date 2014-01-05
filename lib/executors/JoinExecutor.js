/**
 * Query Executor for simple type of queries.
 */
var curry = require('curry');
var filterBuilder = require('../FilterBuilder');
var aggregationHelper = require('../AggregationHelper').AggregationHelper();
var messageBuilderBuilder = require('../builders/QueryMessageBuilder');

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
			switch (message.payloadType) {
			case "DATA":
				this.updateQueryHolderInformation(queryId, message.payload);
				break;
			case "SEC_DATA":
				this.updateQueryHolderInformationWithSecondData(queryId, message.payload);
				break;
			}
		} else {
			// Already sent to the client, do nothing
		}
	} else {
		if (this.runningQueries[queryId] === undefined) {
			if (message.payloadType === "DATA") {
				this.storageApis.send(message.origin, message, function() {
				});
			} else if (message.payloadType === "CONFIG") {
				if (message.payload[datasearch])
					this.returnDataForSecondPartOfJoin(message.queryId, message.payload, message.origin);
				else
					this.executeNonRegQueryOnRemoteCall(message.queryId, message.payload, message.origin);
			}
		} else {
			switch (message.payloadType) {
			case "DATA":
				if (this.runningQueries[queryId].state === STORE) {
					this.updateQueryHolderInformation(queryId, message.payload);
				} else if (this.runningQueries[queryId].state === DISCARD) {
					this.storageApis.send(message.origin, message, function() {
					});
				}
				break;
			case "SEC_DATA":
				if (this.runningQueries[queryId].state === STORE) {
					this.updateQueryHolderInformationWithSecondData(queryId, message.payload);
				} else if (this.runningQueries[queryId].state === DISCARD) {
					this.storageApis.send(message.origin, message, function() {
					});
				}
				break;
			}
		}
	}

};

/**
 * Function to return data to requester to join tables.
 * 
 * @param queryId
 * @param payload
 * @param origin
 */
JoinExecutor.prototype.returnDataForSecondPartOfJoin = function(queryId, payload, origin) {
	var that = this;
	this.storageApis.Node.lscan(payload.namespace, function(data) {
		var localDataV = Object.keys(localData).map(function(key) {
			values.push(localData[key][this.runningQueries[queryId].configuration.joinPropertyOne]);
			return localData[key];
		});

		var data = localDataV.filter(function(elem) {
			return values.indexOf(elem[prop]) !== -1;
		});

		if (data.length !== 0) {
			var message = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setJoinType().setOrigin(
					origin).setCustomPayloadType("SEC_DATA").setPayload(data).buildMessage();
			that.storageApis.Node.send(origin, JSON.stringify(message), function(data) {
			});
		}
	});
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
		var message = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setJoinType().setOrigin(origin)
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
 * 
 * @param configuration
 *            The configuration to be used for execution.
 * @param queryCallback
 *            The callback to return data at the end.
 * @param idCallback
 *            The callback to return id of the query.
 */
JoinExecutor.prototype.registerNewQuery = function(configuration, queryCallback, idCallback) {
	var that = this;
	this.next = this.next + 1;
	var queryId = this.nodeId + "-join-" + this.next;

	this.runningQueries[queryId] = _buildMemory(configuration, queryCallback, STORE);

	if (configuration.timeout !== undefined) {
		configuration.timeout = configuration.timeout * 1000;
	}

	this.statisticHolder.updateStatistics(this.statisticHolder.NEW_QUERY);

	filterBuilder.createFilterFromWhereClause(configuration.filterPlan, function(err, dataFilter) {
		that.runningQueries[queryId].dataFilter = dataFilter;
		idCallback(undefined, queryId);
	});
};

/**
 * Helper method to update information in the local cache.
 * 
 * @param queryId
 *            The query id.
 * @param data
 *            Data to be return to the client.
 */
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
/**
 * Helper method to update information in the local cache. Only data for none
 * local source.
 * 
 * @param queryId
 *            The query id.
 * @param data
 *            Data to be inserted in the cache.
 */
JoinExecutor.prototype.updateQueryHolderInformationWithSecondData = function(queryId, data) {
	if (this.runningQueries[queryId]) {
		var both = this.runningQueries[queryId].configuration.type === 3;
		var joinP = this.runningQueries[queryId].configuration.joinPropertyTwo;
		var that = this;
		data.forEach(function(elem) {		
			var others = that.runningQueries[queryId].tempData[elem[joinP]];
			
			for (var i = 0; i < others.length; i++) {
				that.runningQueries[queryId].currentNumObjects = that.runningQueries[queryId].currentNumObjects + 1;
				that.runningQueries[queryId].data.push(_combine(
						that.runningQueries[queryId].configuration.namespaceOne, elem,
						that.runningQueries[queryId].configuration.namespaceTwo, others[i]));
			}
			if (both) {
				that.runningQueries[queryId].currentNumObjects = that.runningQueries[queryId].currentNumObjects + 1;
				that.runningQueries[queryId].data.push(_combine(
						this.runningQueries[queryId].configuration.namespaceOne, elem, undefined, undefined));
			}
		});
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
 * 
 * @param queryId
 *            The query id.
 */
JoinExecutor.prototype.updateWithLocalMatch = function(queryId) {
	var that = this;
	
	this.storageApis.Node.lscan(this.runningQueries[queryId].configuration.namespaceOne, function(localData) {
		
		if (localData.length !== 0) {
			var values = [];
			var localDataV = Object.keys(localData).map(function(key) {
				values.push(localData[key][that.runningQueries[queryId].configuration.joinPropertyOne]);
				return localData[key];
			});
			localDataV.forEach(function(elem) {
				var id = elem[that.runningQueries[queryId].configuration.joinPropertyOne];
				if (that.runningQueries[queryId].tempData[id] !== undefined) {
					that.runningQueries[queryId].tempData[id].push(elem);
				} else {
					that.runningQueries[queryId].tempData[id] = [ elem ];
				}
			});
			if (that.runningQueries[queryId].configuration.type !== 0) {
				var properData = localDataV.map(function(elem) {
					_combine(that.runningQueries[queryId].configuration.namespaceOne, elem, undefined, undefined);
				});
				that.runningQueries[queryId].currentNumObjects = that.runningQueries[queryId].currentNumObjects
						+ properData.length;
				that.runningQueries[queryId].data = that.runningQueries[queryId].data.concat(properData);
			}
			_matchingDataSearch(queryId, that.runningQueries[queryId].configuration.namespaceTwo,
					that.runningQueries[queryId].configuration.joinPropertyTwo, values, that);
		}
		;
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
JoinExecutor.prototype.returnInformation = function(queryId, callback) {
	if (this.runningQueries[queryId]) {
		this.runningQueries[queryId].state = DISCARD;
		_removeQueryFromMemory(queryId, this.runningQueries[queryId].timeoutValue, this);

		this.statisticHolder.updateStatistics(this.statisticHolder.FIN_QUERY);
		var performance = (new Date()) - this.runningQueries[queryId].beginingTime;
		this.statisticHolder.updateStatistics(this.statisticHolder.QUERY_PERF, performance);

		var filter = this.runningQueries[queryId].dataFilter;
		console.log(this.runningQueries[queryId].data)
		var realData = this.runningQueries[queryId].data.filter(function(elem) {
			return filter(elem);
		});
		callback(undefined, realData);
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
JoinExecutor.prototype.formatSelectProperties = function(selectProperties, callback) {
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

var _buildMemory = function(configuration, queryCallback, state) {
	var temp = {};
	temp.data = [];
	temp.tempData = {};
	temp.configuration = configuration;
	temp.currentNumObjects = 0;
	temp.state = state;
	temp.queryCallback = queryCallback;
	temp.beginingTime = new Date();
	return temp;
};

var _combine = function(namespaceA, elemA, namespaceB, elemB) {
	var data = {};
	if (elemA)
		for (key in elemA) {
			var id = namespaceA + '.' + key;
			data[id] = elemA[key];
		}
	if (elemB)
		for (key in elemB) {
			var id = namespaceB + '.' + key;
			data[id] = elemB[key];
		}
	return data;
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

var _matchingDataSearch = function(queryId, namespace, prop, values, context) {
	var payload = {
		'namespace' : namespace,
		'prop' : prop,
		'values' : values,
		'datasearch' : true
	};

	var message = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setJoinType().setOrigin(
			context.nodeId).setPayloadTypeConfig().setPayload(payload).buildMessage();

	// Should be PHT search
	context.storageApis.Node.broadcast(JSON.stringify(message));
};

module.exports.JoinExecutor = JoinExecutor;