/**
 * Query Executor for join type of queries.
 * 
 * @author Tomo Simeonov
 */
var curry = require('curry');
var underscore = require('underscore');

var filterBuilder = require('../FilterBuilder');
var messageBuilderBuilder = require('../builders/QueryMessageBuilder');
var emitterUtilities = require('../EmitterUtilities');
var utilities = require('./common/Utilities');
var logging = require('./common/Logging');

var STORE = 0;
var DISCARD = 1;

var SEC_DATA = "SEC_DATA";
var PAYLOAD_TYPE_DATA_SEARCH = "DATASEARCH";

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
JoinExecutor.prototype.executeQuery = function(configuration, dataReceiverEmitter) {
	var that = this;
	this.registerNewQuery(configuration, dataReceiverEmitter, function(err, queryId) {
		if (err) {
			emitterUtilities.emitError(dataReceiverEmitter, err);
			emitterUtilities.emitQueryFinish(dataReceiverEmitter);
		} else {
			emitterUtilities.emitExecuting(dataReceiverEmitter, queryId);
			that.queryConfigurations[queryId].configuration.timeoutValue = utilities.setTimeouts(queryId,
					that.queryConfigurations[queryId].queryCallback,
					that.queryConfigurations[queryId].configuration.maxObjects,
					that.queryConfigurations[queryId].configuration.timeout, that);
			that.distribute(queryId);
			that.updateWithLocalMatch(queryId);
		}

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

	logging.logMessageReceivedEvent(this.statisticHolder, message, queryId);
	if (message.payloadType === PAYLOAD_TYPE_DATA_SEARCH) {
		processDataSearchRequrest(queryId, message, this);
	} else if (message.origin === this.nodeId) {
		if (this.queryConfigurations[queryId] !== undefined && this.queryConfigurations[queryId].state === STORE) {
			processMessageContainingDataOriginEnd(queryId, message, this);
		} else {
			// Already sent to the client, do nothing
		}
	} else {
		if (this.queryConfigurations[queryId] === undefined) {
			if (message.payloadType === messageBuilderBuilder.PAYLOAD_TYPE_DATA) {
				this.storageApis.Node.send(message.origin, JSON.stringify(message), function() {
				});
			} else if (message.payloadType === messageBuilderBuilder.PAYLOAD_TYPE_CONFIG) {
				processUnknownConfigQuery(queryId, message, this);
			}
		} else {
			processMessageContainingDataNonOriginEnd(queryId, message, this);
		}
	}

};

// Message handling helper function
var processDataSearchRequrest = function(queryId, message, context) {
	var senderReturn = function(data) {
		if (data.length !== 0) {
			var messageB = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setJoinType().setOrigin(
					message.origin).setCustomPayloadType(SEC_DATA);
			utilities.sendDataMessage(message.origin,messageB,data,context.storageApis);

		}
	};

	context.filterDataOnPropValues(message.queryId, message.payload.namespace, message.payload.prop,
			message.payload.values, senderReturn);
};

// Message handling helper function
var processMessageContainingDataOriginEnd = function(queryId, message, context) {
	switch (message.payloadType) {
	case messageBuilderBuilder.PAYLOAD_TYPE_DATA:
		if (context.queryConfigurations[queryId] !== undefined) {

			context.queryConfigurations[queryId].unmarshaller(message.payload, function(values) {
				context.updateQueryHolderInformation(queryId, values);
			});
		}
		break;
	case SEC_DATA:
		context.updateQueryHolderInformationWithSecondData(queryId, message.payload);
		break;
	}
};

// Message handling helper function
var processMessageContainingDataNonOriginEnd = function(queryId, message, context) {
	switch (message.payloadType) {
	case messageBuilderBuilder.PAYLOAD_TYPE_DATA:
		if (context.queryConfigurations[queryId].state === STORE) {
			contenxt.queryConfigurations[queryId].unmarshaller(message.payload, function(data) {
				context.updateQueryHolderInformation(queryId, data);
			});
		} else if (context.queryConfigurations[queryId].state === DISCARD) {
			context.storageApis.Node.send(message.origin, JSON.stringify(message), function() {
			});
		}
		break;
	case SEC_DATA:
		if (context.queryConfigurations[queryId].state === STORE) {
			context.updateQueryHolderInformationWithSecondData(queryId, message.payload);
		} else if (context.queryConfigurations[queryId].state === DISCARD) {
			context.storageApis.Node.send(message.origin, JSON.stringify(message), function() {
			});
		}
		break;
	}
};

// Message handling helper function
var processUnknownConfigQuery = function(queryId, message, context) {
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
	var that = this;
	this.storageApis.Node.lscan(namespace, function(localData) {
		logging.logLocalDataAccessEvent(that.statisticHolder, namespace, Object.keys(localData).length, queryId);
		var data = {};
		for ( var key in localData) {
			if (values.indexOf(localData[key][joinProp]) !== -1) {
				data[key] = localData[key];
			}
		}
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
			var messageB = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setJoinType().setOrigin(
					origin).setPayloadTypeData();
			utilities.sendDataMessage(origin,messageB,data,that.storageApis);
		}
	});

	var callSender = callSenderPrototype(origin);
	// Getting schemas
	_getschemas(configuration.namespaceOne, configuration.namespaceTwo, this.storageApis, function(err, schemaOne,
			schemaTwo) {
		if (err) {
			logging.logRemoteErrorOnMissingschema(that.statisticHolder, configuration.namespace, queryId);
		} else {
			that.runningData[queryId] = {};

			that.queryConfigurations[queryId] = _buildMemoryForRemote(configuration, callSender, STORE);
			that.queryConfigurations[queryId].configuration.timeoutValue = utilities.setTimeouts(queryId, callSender,
					-1, configuration.timeoutValue / 3.0, that);

			utilities.produceUnmarshallerDataFunction(schemaOne, undefined, function(unmarshaller) {
				that.queryConfigurations[queryId].unmarshaller = unmarshaller;
				filterBuilder.createFilterFromWhereClause(configuration.filterPlan, function(err, dataFilter) {
					that.queryConfigurations[queryId].dataFilter = dataFilter;
					that.updateWithLocalMatch(queryId);
					that.statisticHolder.emitEvent(that.statisticHolder.NEW_QUERY);
				});
			}, schemaTwo);
		}
	});

};

/**
 * Registers new query to be executed in the network with this node the origin.
 * 
 * @param configuration
 *            The configuration to be used for execution.
 * @param dataReceiverEmitter
 *            The callback to return data at the end.
 * @param idCallback
 *            The callback to return id of the query.
 */
JoinExecutor.prototype.registerNewQuery = function(configuration, dataReceiverEmitter, idCallback) {
	var that = this;
	this.next = this.next + 1;
	var queryId = this.nodeId + "-join-" + this.next;

	// Getting schemas
	_getschemas(configuration.namespaceOne, configuration.namespaceTwo, this.storageApis, function(err, schemaOne,
			schemaTwo) {
		if (err) {
			idCallback(err, queryId);
		} else {

			that.queryConfigurations[queryId] = _buildMemory(configuration, dataReceiverEmitter, STORE);
			that.runningData[queryId] = {};

			if (configuration.timeout !== undefined) {
				configuration.timeout = configuration.timeout * 1000;
			}

			that.statisticHolder.emitEvent(that.statisticHolder.NEW_QUERY);

			utilities.produceUnmarshallerDataFunction(schemaOne, undefined, function(unmarshaller) {
				that.queryConfigurations[queryId].unmarshaller = unmarshaller;
				filterBuilder.createFilterFromWhereClause(configuration.filterPlan, function(err, dataFilter) {
					that.queryConfigurations[queryId].dataFilter = dataFilter;
					idCallback(undefined, queryId);
				});
			}, schemaTwo);
		}
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
		utilities.processingQueryData(queryId, data, false, that);
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
	if (this.queryConfigurations[queryId] && Object.keys(data).length !== 0) {
		var shouldKeppOnlyThisToo = this.queryConfigurations[queryId].configuration.type === 3
				|| this.queryConfigurations[queryId].configuration.type === 2;
		var joinP = this.queryConfigurations[queryId].configuration.joinPropertyTwo;
		var that = this;

		var realData = {};

		for ( var key in data) {

			var others = that.queryConfigurations[queryId].tempData[data[key][joinP]];

			if (shouldKeppOnlyThisToo) {
				realData[key] = _combine(that.queryConfigurations[queryId].configuration.namespaceTwo, data[key],
						undefined, undefined);
			}

			for ( var secondKey in others) {
				realData["" + key + "-" + secondKey] = _combine(
						that.queryConfigurations[queryId].configuration.namespaceTwo, data[key],
						that.queryConfigurations[queryId].configuration.namespaceOne, others[secondKey]);
			}

		}

		var filter = this.queryConfigurations[queryId].dataFilter;

		this.queryConfigurations[queryId].unmarshaller(realData, function(realData) {
			utilities.filterData(realData, filter, undefined, undefined, function(data, size) {
				utilities.processingQueryData(queryId, data, false, that);
			});
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

	var localMatchHelperProces = function(localData) {
		var objectKeys = Object.keys(localData);
		logging.logLocalDataAccessEvent(that.statisticHolder,
				that.queryConfigurations[queryId].configuration.namespaceOne, objectKeys.length, queryId);

		if (objectKeys.length !== 0) {
			var values = [];
			var singleTableResult = {};
			var shouldKeepIfNoOtherNamespaceMatch = that.queryConfigurations[queryId].configuration.type !== 0
					&& that.queryConfigurations[queryId].configuration.type !== 2;

			for ( var key in localData) {
				var id = localData[key][that.queryConfigurations[queryId].configuration.joinPropertyOne];

				// Values of join prop are extract for later use
				values.push(id);

				// Updating waiting table for matching namespace two
				if (that.queryConfigurations[queryId].tempData[id] !== undefined) {
					that.queryConfigurations[queryId].tempData[id][key] = localData[key];
				} else {
					that.queryConfigurations[queryId].tempData[id] = {};
					that.queryConfigurations[queryId].tempData[id][key] = localData[key];

				}

				// If should keep namespace even if no other match
				if (shouldKeepIfNoOtherNamespaceMatch) {
					singleTableResult[key] = _combine(that.queryConfigurations[queryId].configuration.namespaceOne,
							localData[key], undefined, undefined);
				}
			}

			var filter = that.queryConfigurations[queryId].dataFilter;

			if (shouldKeepIfNoOtherNamespaceMatch && Object.keys(singleTableResult).length) {
				that.queryConfigurations[queryId].unmarshaller(singleTableResult, function(singleTableResult) {
					utilities.filterData(singleTableResult, filter, undefined, undefined, function(data, size) {
						utilities.processingQueryData(queryId, data, false, that);
					});
				});
			}

			_matchingDataSearch(queryId, that.queryConfigurations[queryId].configuration.namespaceTwo,
					that.queryConfigurations[queryId].configuration.joinPropertyTwo, values, that);

			that.filterDataOnPropValues(queryId, that.queryConfigurations[queryId].configuration.namespaceTwo,
					that.queryConfigurations[queryId].configuration.joinPropertyTwo, values, function(data) {
						that.updateQueryHolderInformationWithSecondData(queryId, data);
					});
		}
	};

	this.storageApis.Node.lscan(this.queryConfigurations[queryId].configuration.namespaceOne, localMatchHelperProces);
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
		logging.updatingStatisticsOnFinishingQuery(performance, this.statisticHolder);

		if (this.queryConfigurations[queryId].queryCallback) {

			if (Object.keys(this.runningData[queryId]).length !== 0) {
				logging.logDataDeliveredEvent(this.statisticHolder, Object.keys(this.runningData[queryId]).length,
						this.queryConfigurations[queryId].beginingTime, queryId);
				this.queryConfigurations[queryId].queryCallback(undefined, this.runningData[queryId]);
			}

		} else {
			emitterUtilities.emitQueryFinish(this.queryConfigurations[queryId].dataReceiverEmitter);
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
		var size = 0;

		var data = selectProperties.map(function(elem) {
			var el = elem.split('.');
			if (el.length !== 2 || el.indexOf('(') !== -1) {
				return {};
			} else {
				var data = {};
				data[el[0]] = el[1];
				return data;
			}
		}).reduce(function(prev, curr) {
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
		//More than two tables?
		if (size <= 2) {
			callback(undefined, data);
		} else {
			callback(new Error("Not recognized syntaxis in select."), undefined);
		}
	}
};

// Utility methods

var _buildMemory = function(configuration, dataReceiverEmitter, state) {
	var temp = {};
	temp.data = {};
	temp.keys = {};
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

//Combines a value with other value in new dict
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
			if (elemB)
				data[id] = elemB[key];
		}
	return data;
};

var _getschemas = function(namespaceOne, namespaceTwo, storageApis, callback) {
	utilities.getschema(namespaceOne, storageApis, function(err, schemaOne) {
		if (err) {
			callback(err, undefined, undefined);
		} else {
			utilities.getschema(namespaceTwo, storageApis, function(err, schemaTwo) {
				if (err) {
					callback(err, undefined, undefined);
				} else {
					callback(undefined, schemaOne, schemaTwo);
				}
			});
		}
	});
};

//TODO PHT Support
var _matchingDataSearch = function(queryId, namespace, prop, values, context) {

	var payload = {
		'namespace' : namespace,
		'prop' : prop,
		'values' : values
	};

	var message = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setJoinType().setOrigin(
			context.nodeId).setCustomPayloadType(PAYLOAD_TYPE_DATA_SEARCH).setPayload(payload).buildMessage();

	// Should be PHT search
	context.storageApis.Node.broadcast(JSON.stringify(message));
};

module.exports.JoinExecutor = JoinExecutor;