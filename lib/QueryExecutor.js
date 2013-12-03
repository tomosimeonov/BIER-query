/**
 * New node file
 */

var filterBuilder = require('./FilterBuilder');
var aggregationHelper = require('./AggregationHelper').AggregationHelper();

function QueryExecutor(storageApis) {
	var self = this instanceof QueryExecutor ? this : Object.create(QueryExecutor.prototype);
	self.storageApis = storageApis;
	self.runningQueries = [];
	self.date = new Date();
	self.defaultObjectTimeoutInMillis = 1000;
	self.defaultTimeoutInMillis = 3000;
	return self;
}

/** Default time timeout if no type of timeout provided */
QueryExecutor.prototype.objectCountStopperTimeout = 5000;

QueryExecutor.prototype.upcall = function(data) {
	//TODO handle calls
};

QueryExecutor.prototype.sendQuery = function(destination, queryId) {
	//TODO distribute across the network
};

QueryExecutor.prototype.updateQueryHolderInformation = function(queryId, data) {
	if (this.runningQueries[queryId] !== undefined) {
		this.runningQueries[queryId].data = this.runningQueries[queryId].data.concat(data);
		this.runningQueries[queryId].currentNumObjects = this.runningQueries[queryId].currentNumObjects + data.length;
		if (this.runningQueries[queryId].currentNumObjects === this.runningQueries[queryId].configuration.maxObjects) {
			// Have to terminate
			this.returnInformation(queryId, this.runningQueries[queryId].queryCallback);
		} else if (this.runningQueries[queryId].configuration.maxObjects !== -1) {
			// Restart timer for object count
			this.objectCountStopper(queryId, this.runningQueries[queryId].currentNumObjects,
					this.runningQueries[queryId].queryCallback);
		}
	}
};

QueryExecutor.prototype.registerNewQuery = function(configuration, queryCallback, idCallback) {
	var that = this;
	try {
		var randomNumber = Math.floor((Math.random() * 100) + 1);
		var currentTime = this.date.getTime();

		var queryId = randomNumber + "-" + currentTime;
		this.runningQueries[queryId] = {};
		this.runningQueries[queryId].data = [];
		this.runningQueries[queryId].configuration = configuration;
		this.runningQueries[queryId].currentNumObjects = 0;
		this.runningQueries[queryId].queryCallback = queryCallback;

		// Set up object count timeout if any
		if (configuration.maxObjects !== -1) {
			this.objectCountStopper(queryId, 0, queryCallback);
		}

		// Set up timeout if any
		if (configuration.timeout !== undefined) {
			setTimeout(function() {
				that.returnInformation(queryId, queryCallback);
			}, configuration.timeout * 1000);
		}

		// Default timeout if non provided
		if (configuration.maxObjects === -1 && configuration.timeout === undefined) {
			setTimeout(function() {
				that.returnInformation(queryId, queryCallback);
			}, defaultTimeoutInMillis);
		}
		idCallback(undefined, queryId);
	} catch (err) {
		idCallback(err, undefined);
	}
};

QueryExecutor.prototype.updateWithLocalMatch = function(queryId) {
	var that = this;
	var namespace = this.runningQueries[queryId].configuration.namespace;
	var properties = this.runningQueries[queryId].configuration.properties;
	var filterPlan = this.runningQueries[queryId].configuration.filterPlan;

	var localData = this.storageApis.lscan(namespace);
	filterBuilder.createFilterFromWhereClause(filterPlan, function(err, dataFilter) {
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
	if (this.runningQueries[queryId]) {
		var queryData = this.runningQueries[queryId];
		delete this.runningQueries[queryId];
		var data = queryData.data;
		if (queryData.configuration.aggregations.length !== 0 && data.length !== 0) {
			aggregationHelper.processData(data, queryData.configuration.properties,
					queryData.configuration.aggregations, callback);
		} else {
			callback(undefined, data);
		}
	}
};

// Utility methods
QueryExecutor.prototype.objectCountStopper = function(queryId, prevObjCount, queryCallback) {
	var that = this;
	setTimeout(function() {
		if (that.runningQueries[queryId] !== undefined
				&& that.runningQueries[queryId].currentNumObjects === prevObjCount)
			that.returnInformation(queryId, queryCallback);
	}, that.defaultObjectTimeoutInMillis);
};

QueryExecutor.prototype.formatSelectProperties = function(selectProperties, callback) {
	aggregationHelper.propertiesProcessor(selectProperties, callback);
};

module.exports.QueryExecutor = QueryExecutor;