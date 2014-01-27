/**
 * Query Executor for simple type of queries.
 */
var curry = require('curry');
var filterBuilder = require('../FilterBuilder');
var aggregationHelper = require('../AggregationHelper').AggregationHelper();
var messageBuilderBuilder = require('../builders/QueryMessageBuilder');
var utilities = require('./ExecutorUtilities');

var STORE = 0;
var DISCARD = 1;

function JoinExecutor(storageApis, statisticHolder) {
	var self = this instanceof JoinExecutor ? this : Object.create(JoinExecutor.prototype);
	self.storageApis = storageApis;
	self.statisticHolder = statisticHolder;

	self.queryConfigurations = [];
	self.runningData = [];
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
JoinExecutor.prototype.executeQuery = function(configuration, queryCallback) {
	var that = this;
	this.registerNewQuery(configuration, queryCallback, function(err, queryId) {
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
 *            Message to process
 */
JoinExecutor.prototype.message = function(message) {
	var queryId = message.queryId;

	if (message.origin === this.nodeId) {
		if (this.queryConfigurations[queryId] !== undefined && this.queryConfigurations[queryId].state === STORE) {
			processMessageContainingDataOriginEnd(queryId, message, this);
		} else {
			// Already sent to the client, do nothing
		}
	} else {
		if (this.queryConfigurations[queryId] === undefined) {
			if (message.payloadType === "DATA") {
				this.storageApis.send(message.origin, message, function() {
				});
			} else if (message.payloadType === "CONFIG") {
				processUnknownConfigQuery(queryId, message, this);
			}
		} else {
			processMessageContainingDataNonOriginEnd(queryId, message, this);
		}
	}

};

//Message handling function
var processMessageContainingDataOriginEnd = function(queryId, message, context) {
	switch (message.payloadType) {
	case "DATA":
		context.updateQueryHolderInformation(queryId, message.payload);
		break;
	case "SEC_DATA":
		context.updateQueryHolderInformationWithSecondData(queryId, message.payload);
		break;
	}
};

//Message handling function
var processMessageContainingDataNonOriginEnd = function(queryId, message, context) {
	switch (message.payloadType) {
	case "DATA":
		if (context.queryConfigurations[queryId].state === STORE) {
			context.updateQueryHolderInformation(queryId, message.payload);
		} else if (context.queryConfigurations[queryId].state === DISCARD) {
			context.storageApis.send(message.origin, message, function() {
			});
		}
		break;
	case "SEC_DATA":
		if (context.queryConfigurations[queryId].state === STORE) {
			context.updateQueryHolderInformationWithSecondData(queryId, message.payload);
		} else if (context.queryConfigurations[queryId].state === DISCARD) {
			context.storageApis.send(message.origin, message, function() {
			});
		}
		break;
	}
};

//Message handling function
var processUnknownConfigQuery = function(queryId, message, context) {
	if (message.payload.datasearch !== undefined) {
		var senderReturn = function(data) {
			if (data.length !== 0) {
				var newMessage = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setJoinType()
						.setOrigin(message.origin).setCustomPayloadType("SEC_DATA").setPayload(data).buildMessage();
				context.storageApis.Node.send(origin, JSON.stringify(newMessage), function(data) {
				});
			}
		};

		context.filterDataOnPropValues(message.queryId, message.payload.namespace, message.payload.prop,
				message.payload.values, senderReturn);
	} else
		context.executeNonRegQueryOnRemoteCall(message.queryId, message.payload, message.origin);
};

/**
 * Function to return data to requester to join tables.
 * 
 * @param queryId
 * @param payload
 * @param origin
 */
JoinExecutor.prototype.filterDataOnPropValues = function(queryId, namespace, joinProp, values, callback) {
	this.storageApis.Node.lscan(namespace, function(localData) {
		var localDataV = Object.keys(localData).map(function(key) {
			return localData[key];
		});

		var data = localDataV.filter(function(elem) {
			return values.indexOf(elem[joinProp]) !== -1;
		});

		callback(data);

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
			.setPayloadTypeConfig().setPayload(this.queryConfigurations[queryId].configuration).buildMessage();
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
		if (data.length !== 0) {
			var message = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setJoinType().setOrigin(
					origin).setPayloadTypeData().setPayload(data).buildMessage();
			that.storageApis.Node.send(origin, JSON.stringify(message), function(data) {
			});
		}
	});

	var callSender = callSenderPrototype(origin);

	this.runningData[queryId] = [];

	this.queryConfigurations[queryId] = _buildMemoryForRemote(configuration, callSender, STORE);
	this.queryConfigurations[queryId].configuration.timeoutValue = utilities.setTimeouts(queryId, callSender, -1,
			configuration.timeoutValue / 5, this);

	filterBuilder.createFilterFromWhereClause(configuration.filterPlan, function(err, dataFilter) {
		that.queryConfigurations[queryId].dataFilter = dataFilter;
		that.updateWithLocalMatch(queryId);
		that.statisticHolder.updateStatistics(that.statisticHolder.NEW_QUERY);
	});

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

	this.queryConfigurations[queryId] = _buildMemory(configuration, queryCallback, STORE);
	this.runningData[queryId] = [];

	if (configuration.timeout !== undefined) {
		configuration.timeout = configuration.timeout * 1000;
	}

	this.statisticHolder.updateStatistics(this.statisticHolder.NEW_QUERY);

	filterBuilder.createFilterFromWhereClause(configuration.filterPlan, function(err, dataFilter) {
		that.queryConfigurations[queryId].dataFilter = dataFilter;
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
	var that = this;
	if (this.queryConfigurations[queryId] !== undefined) {
		utilities.filterData(queryId, data, filter, function(queryId, data) {
			utilities.processingQueryData(queryId, data, false, that);
		});
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
	if (this.queryConfigurations[queryId]) {
		var both = this.queryConfigurations[queryId].configuration.type === 3;
		var joinP = this.queryConfigurations[queryId].configuration.joinPropertyTwo;
		var that = this;

		var realData = [];
		// Rewrite that to make it work better
		data.forEach(function(elem) {
			var others = that.queryConfigurations[queryId].tempData[elem[joinP]];

			for (var i = 0; i < others.length; i++) {
				realData.push(_combine(that.queryConfigurations[queryId].configuration.namespaceOne, elem,
						that.queryConfigurations[queryId].configuration.namespaceTwo, others[i]));
			}
			if (both) {
				realData.push(_combine(that.queryConfigurations[queryId].configuration.namespaceOne, elem, undefined,
						undefined));
			}
		});

		var filter = this.queryConfigurations[queryId].dataFilter;
		utilities.filterData(queryId, realData, filter, function(queryId, data) {
			utilities.processingQueryData(queryId, data, false, that);
		});
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

	var temp = function(localData) {
		if (localData.length !== 0) {
			var values = [];
			var localDataV = Object.keys(localData).map(function(key) {
				values.push(localData[key][that.queryConfigurations[queryId].configuration.joinPropertyOne]);
				return localData[key];
			});
			localDataV.forEach(function(elem) {
				var id = elem[that.queryConfigurations[queryId].configuration.joinPropertyOne];
				if (that.queryConfigurations[queryId].tempData[id] !== undefined) {
					that.queryConfigurations[queryId].tempData[id].push(elem);
				} else {
					that.queryConfigurations[queryId].tempData[id] = [ elem ];
				}
			});
			if (that.queryConfigurations[queryId].configuration.type !== 0) {
				var properData = localDataV.map(function(elem) {
					_combine(that.queryConfigurations[queryId].configuration.namespaceOne, elem, undefined, undefined);
				});
				that.queryConfigurations[queryId].currentNumObjects = that.queryConfigurations[queryId].currentNumObjects
						+ properData.length;
				that.runningData[queryId] = that.runningData[queryId].concat(properData);
			}
			_matchingDataSearch(queryId, that.queryConfigurations[queryId].configuration.namespaceTwo,
					that.queryConfigurations[queryId].configuration.joinPropertyTwo, values, that);

			that.filterDataOnPropValues(queryId, that.queryConfigurations[queryId].configuration.namespaceTwo,
					that.queryConfigurations[queryId].configuration.joinPropertyTwo, values, function(data) {
						that.updateQueryHolderInformationWithSecondData(queryId, data);
					});
		}
	};

	this.storageApis.Node.lscan(this.queryConfigurations[queryId].configuration.namespaceOne, temp);
};

/**
 * Helper method to return data to the client.
 * 
 * @param queryId
 *            The query id.
 * @param callback
 *            The callback to return to.
 */
JoinExecutor.prototype.finish = function(queryId) {
	if (this.queryConfigurations[queryId]) {
		this.queryConfigurations[queryId].state = DISCARD;
		utilities.removeQueryFromMemory(queryId, this.queryConfigurations[queryId].timeoutValue, this);

		var performance = (new Date()) - this.queryConfigurations[queryId].beginingTime;
		utilities.updatingStatisticsOnFinishingQuery(performance, this.statisticHolder);

		if (this.queryConfigurations[queryId].queryCallback) {
			this.queryConfigurations[queryId].queryCallback(undefined, this.runningData[queryId]);
		} else {
			this.queryConfigurations[queryId].dataReceiverEmitter.emit('end');
		}

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

var _buildMemory = function(configuration, dataReceiverEmitter, state) {
	var temp = {};
	temp.data = [];
	temp.tempData = {};
	temp.configuration = configuration;
	temp.currentNumObjects = 0;
	temp.state = state;
	temp.queryCallback = undefined;
	temp.dataReceiverEmitter = dataReceiverEmitter;
	temp.beginingTime = new Date();
	return temp;
};

var _buildMemoryForRemote = function(configuration, queryCallback, state) {
	var remoteBuild = _buildMemory(configuration, undefined, state);
	remoteBuild.queryCallback = queryCallback;
	return remoteBuild;
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