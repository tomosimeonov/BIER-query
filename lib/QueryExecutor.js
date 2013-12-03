/**
 * New node file
 */

function QueryExecutor(storageApis) {
	var self = this instanceof QueryExecutor ? this : Object.create(QueryExecutor.prototype);
	self.storageApis = storageApis;
	self.runningQueries = [];
	self.date = new Date();
	self.filterBuilder = require('./FilterBuilder');
	self.aggregationHelper = require('./AggregationHelper').AggregationHelper();
	return self;
}
/**
 * Timeout to cancel execution when wanting objects number and no new appear for
 * that time
 */
QueryExecutor.prototype.objectCountStopperTimeout = 1000;

/** Default time timeout if no type of timeout provided */
QueryExecutor.prototype.objectCountStopperTimeout = 5000;

QueryExecutor.prototype.upcall = function(data) {

};

QueryExecutor.prototype.sendData = function(destination, queryId, data) {

};

QueryExecutor.prototype.updateQueryHolderInformation = function(queryId, data) {
	if (this.runningQueries[queryId] !== undefined) {
		this.runningQueries[queryId].data = this.runningQueries[queryId].data.concat(data);
		this.runningQueries[queryId].currentNumObjects = this.runningQueries[queryId].currentNumObjects + data.length;
		if (this.runningQueries[queryId].currentNumObjects === this.runningQueries[queryId].maxObjects) {
			// Have to terminate
			this.returnInformation(queryId, this.runningQueries[queryId].queryCallback);
		} else if (this.runningQueries[queryId].maxObjects !== -1) {
			// Restart timer for object count
			this.objectCountStopper(queryId, this.runningQueries[queryId].currentNumObjects,
					this.runningQueries[queryId].queryCallback);
		}
	}
};

QueryExecutor.prototype.registerNewQuery = function(queryConfiguration, queryCallback, idCallback) {
	var that = this;
	try {
		var randomNumber = Math.floor((Math.random() * 100) + 1);
		var currentTime = this.date.getTime();

		var queryId = randomNumber + "-" + currentTime;
		this.runningQueries[queryId] = {};
		this.runningQueries[queryId].data = [];
		this.runningQueries[queryId].aggreg = queryConfiguration.aggregations;
		this.runningQueries[queryId].maxObjects = -1;
		this.runningQueries[queryId].currentNumObjects = 0;
		this.runningQueries[queryId].queryCallback = queryCallback;
		this.runningQueries[queryId].properties = queryConfiguration.properties;
		this.runningQueries[queryId].filterPlan = queryConfiguration.filterPlan;
		this.runningQueries[queryId].namespace = queryConfiguration.namespace;

		if (queryConfiguration.maxObjects !== undefined) {
			this.runningQueries[queryId].maxObjects = queryConfiguration.maxObjects;
			this.objectCountStopper(queryId, 0, queryCallback);
		}
		if (queryConfiguration.timeout !== undefined) {
			// Setup the timeout handler so at the end to inform the client
			setTimeout(function() {
				that.returnInformation(queryId, queryCallback);
			}, queryConfiguration.timeout * 1000);
		}
		if (queryConfiguration.maxObjects !== undefined && queryConfiguration.timeout !== undefined) {
			// Setup the timeout handler so at the end to inform the client
			// default
			// value of 5 sec
			setTimeout(function() {
				that.returnInformation(queryId, queryCallback);
			}, 5000);
		}
		idCallback(undefined, queryId);
	} catch (err) {
		idCallback(err, undefined);
	}
};

QueryExecutor.prototype.updateWithLocalMatch = function(queryId) {
	var that = this;
	var namespace = this.runningQueries[queryId].namespace;
	var properties = this.runningQueries[queryId].properties;
	var filterPlan = this.runningQueries[queryId].filterPlan;
	
	var localData = this.storageApis.lscan(namespace);
	this.filterBuilder.createFilterFromWhereClause(filterPlan, function(err, dataFilter) {
		var filteredData = localData.filter(dataFilter);
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

};

QueryExecutor.prototype.returnInformation = function(queryId, callback) {
	var queryData = this.runningQueries[queryId];
	delete this.runningQueries[queryId];
	var data = queryData.data;
	if (queryData.aggreg.length !== 0 && data.length !== 0) {
		this.aggregationHelper.processData(data, queryData.properties, queryData.aggreg, callback);
	} else {
		callback(undefined, data);
	}

};

// Utility methods
QueryExecutor.prototype.objectCountStopper = function(queryId, prevObjCount, queryCallback) {
	var that = this;
	setTimeout(function() {
		if (that.runningQueries[queryId] !== undefined
				&& that.runningQueries[queryId].currentNumObjects === prevObjCount)
			that.returnInformation(queryId, queryCallback);
	}, that.objectCountStopperTimeout);
};

QueryExecutor.prototype.formatSelectProperties = function(selectProperties, callback) {
	this.aggregationHelper.propertiesProcessor(selectProperties, callback);
};

module.exports.QueryExecutor = QueryExecutor;