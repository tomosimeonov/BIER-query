/**
 * Simple Query Executor for simple type of queries.
 * 
 * @author Tomo Simeonov
 */

// Local imports
var filterBuilder = require('../FilterBuilder');
var messageBuilderBuilder = require('../builders/QueryMessageBuilder');
var aggregationHelper = require('../AggregationHelper').AggregationHelper();
var emitterUtilities = require('../EmitterUtilities');
var utilities = require('./common/Utilities');
var logging = require('./common/Logging');
var constants = require('../Constants');

// 3d Party
var curry = require('curry');
var underscore = require('underscore');

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
SimpleExecutor.prototype.executeQuery = function(configuration, dataReceiverEmitter) {
	var that = this;
	this.registerNewQuery(configuration, dataReceiverEmitter, function(err, queryId) {
		emitterUtilities.emitExecuting(dataReceiverEmitter, queryId);
		if (err) {
			emitterUtilities.emitError(dataReceiverEmitter, err);
			emitterUtilities.emitQueryFinish(dataReceiverEmitter);
		} else {
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
 *            Message to process.
 */
SimpleExecutor.prototype.message = function(message) {
	var queryId = message.queryId;
	logging.logMessageReceivedEvent(this.statisticHolder, message, queryId);
	
	if (message.origin === this.nodeId) {
		if (this.queryConfigurations[queryId] !== undefined && this.queryConfigurations[queryId].state === constants.STORE) {
			var that = this;
			this.queryConfigurations[queryId].unmarshaller(message.payload, function(values) {
				that.updateQueryHolderInformation(queryId, values);
			});

		} else {
			// Already sent to the client, do nothing
		}
	} else {
		if (this.queryConfigurations[queryId] === undefined) {
			if (message.payloadType === constants.DATA_MESSAGE) {
				this.storageApis.Node.send(message.origin, JSON.stringify(message), function() {
				});
			} else if (message.payloadType === constants.CONFIG_MESSAGE) {
				this.executeNonRegQueryOnRemoteCall(message.queryId, message.payload, message.origin);
			}
		} else {
			if (this.queryConfigurations[queryId].state === constants.STORE && message.payloadType === constants.DATA_MESSAGE) {
				this.updateQueryHolderInformation(queryId, message.payload);
			} else if (this.queryConfigurations[queryId].state === constants.DISCARD && message.payloadType === constants.DATA_MESSAGE) {
				this.storageApis.Node.send(message.origin, JSON.stringify(message), function() {
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
	// TODO move destinations out of the configuration object
	logging.logDistributingConfigurationEvent(this.statisticHolder,
			this.queryConfigurations[queryId].configuration.destinations, queryId);
	if (this.queryConfigurations[queryId].configuration.destinations.length === 0) {

		this.storageApis.Node.broadcast(JSON.stringify(message));
	} else {
		// TODO Do for id to id
		this.queryConfigurations[queryId].configuration.destinations.forEach(function(dest) {
			that.storageApis.Node.send(dest, message, function() {
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

		var messageB = messageBuilderBuilder.QueryMessageBuilder().setQueryId(queryId).setSimpleType()
				.setOrigin(origin).setPayloadTypeData();
		utilities.sendDataMessage(origin, messageB, data, that.storageApis);

	});

	var callSender = callSenderPrototype(origin);

	utilities.getschema(configuration.namespace, this.storageApis, function(err, schema) {
		if (err) {
			logging.logRemoteErrorOnMissingschema(that.statisticHolder, configuration.namespace, queryId);
		} else {
			that.statisticHolder.emitEvent(constants.NEW_QUERY);
			that.runningData[queryId] = {};
			that.queryConfigurations[queryId] = _buildMemoryForRemote(configuration, callSender, constants.STORE);
			that.queryConfigurations[queryId].primaryKey = schema.primaryKey;
			that.queryConfigurations[queryId].local = false;

			utilities.produceUnmarshallerDataFunction(schema, configuration.aggregations, function(unmarshaller) {
				that.queryConfigurations[queryId].unmarshaller = unmarshaller;
				that.queryConfigurations[queryId].configuration.timeoutValue = utilities.setTimeouts(queryId,
						callSender, -1, configuration.timeoutValue / 5.0, that);
				that.updateWithLocalMatch(queryId);
			});

		}
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
SimpleExecutor.prototype.registerNewQuery = function(configuration, dataReceiverEmitter, idCallback) {

	this.next = this.next + 1;
	var queryId = this.nodeId + "-simple-" + this.next;
	var that = this;
	utilities.getschema(configuration.namespace, this.storageApis, function(err, schema) {
		if (err) {
			idCallback(err, queryId);
		} else {
			that.statisticHolder.emitEvent(constants.NEW_QUERY);
			that.queryConfigurations[queryId] = _buildMemory(configuration, dataReceiverEmitter, constants.STORE);
			that.queryConfigurations[queryId].primaryKey = schema.primaryKey;
			that.queryConfigurations[queryId].local = true;
			that.runningData[queryId] = {};

			if (configuration.timeout !== undefined) {
				configuration.timeout = configuration.timeout * 1000;
			}

			utilities.produceUnmarshallerDataFunction(schema, configuration.aggregations, function(unmarshaller) {
				that.queryConfigurations[queryId].unmarshaller = unmarshaller;
				idCallback(undefined, queryId);
			});

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
SimpleExecutor.prototype.updateQueryHolderInformation = function(queryId, data) {
	utilities.processingQueryData(queryId, data, this.queryConfigurations[queryId].isAggregation, this);
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
	var unmarshaller = this.queryConfigurations[queryId].unmarshaller;

	this.storageApis.Node.lscan(namespace, function(localData) {
		filterBuilder.createFilterFromWhereClause(filterPlan, function(err, dataFilter) {
			var objectKeys = Object.keys(localData);
			logging.logLocalDataAccessEvent(that.statisticHolder, namespace, objectKeys.length, queryId);

			var transformation = function(currentData, properties) {
				var data = {};
				if (properties.length !== 0) {
					properties.forEach(function(oneProp) {
						data[oneProp] = currentData[oneProp];
					});
				} else {
					data = currentData;
				}
				return data;
			};
			unmarshaller(localData, function(values) {
				utilities.filterData(localData, dataFilter, transformation, properties, function(filteredData, size) {
					if (size > 0) {
						that.updateQueryHolderInformation(queryId, filteredData);
					}
				});

			});
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
SimpleExecutor.prototype.finish = function(queryId) {
	var that = this;
	if (this.queryConfigurations[queryId]) {

		this.queryConfigurations[queryId].state = constants.DISCARD;
		utilities.removeQueryFromMemory(queryId, this.queryConfigurations[queryId].timeoutValue, this);

		var queryConfiguration = this.queryConfigurations[queryId];

		var correctCallback = queryConfiguration.queryCallback;
		if (correctCallback === undefined) {
			correctCallback = function(err, dataa) {
				var data = underscore.values(dataa);
				if (data.length !== 0) {
					logging.logDataDeliveredEvent(that.statisticHolder, data.length,
							that.queryConfigurations[queryId].beginingTime, queryId);
					emitterUtilities.emitData(that.queryConfigurations[queryId].dataReceiverEmitter, data);
				}
				emitterUtilities.emitQueryFinish(that.queryConfigurations[queryId].dataReceiverEmitter);
			};
		} else {
			var temp = correctCallback;
			correctCallback = function(err, data) {
				logging.logDataDeliveredEvent(that.statisticHolder, Object.keys(data).length,
						that.queryConfigurations[queryId].beginingTime, queryId);
				temp(err, data);
			};
		}
		if (queryConfiguration.local == true && queryConfiguration.isAggregation
				&& Object.keys(this.runningData[queryId]).length !== 0) {
			var data = this.runningData[queryId];
			delete this.runningData[queryId];
			aggregationHelper.processData(data, queryConfiguration.configuration.properties,
					queryConfiguration.configuration.aggregations, this.nodeId, correctCallback);
		} else {
			correctCallback(undefined, this.runningData[queryId]);
		}

		var performance = (new Date()) - queryConfiguration.beginingTime;
		logging.updatingStatisticsOnFinishingQuery(performance, this.statisticHolder);
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
var _buildMemory = function(configuration, dataReceiverEmitter, state) {
	var temp = {};
	temp.keys = {};
	temp.configuration = configuration;
	temp.currentNumObjects = 0;
	temp.state = state;
	temp.queryCallback = undefined;
	temp.dataReceiverEmitter = dataReceiverEmitter;
	temp.beginingTime = new Date();
	temp.isAggregation = configuration.aggregations.length !== 0;
	return temp;
};

var _buildMemoryForRemote = function(configuration, queryCallback, state) {
	var remoteBuild = _buildMemory(configuration, undefined, state);
	remoteBuild.queryCallback = queryCallback;
	return remoteBuild;
};

module.exports.SimpleExecutor = SimpleExecutor;