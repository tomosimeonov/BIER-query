/**
 * New node file
 */

function QueryExecutor(storageApis) {
	var self = this instanceof QueryExecutor ? this : Object.create(QueryExecutor.prototype);
	self.storageApis = storageApis;
	self.runningQueries = [];
	self.date = new Date();
	self.filterBuilder = require('./FilterBuilder');
	return self;
}

QueryExecutor.prototype.upcall = function(data) {

};

QueryExecutor.prototype.sendData = function(destination, queryId, data) {

};

QueryExecutor.prototype.updateQueryHolderInformation = function(queryId, data, aggregationData) {
	if (this.runningQueries[queryId] !== undefined) {
		this.runningQueries[queryId].data = this.runningQueries[queryId].data.concat(data);
		this.runningQueries[queryId].currentNumObjects = this.runningQueries[queryId].currentNumObjects + data.length;
		if (aggregationData !== undefined) {
			this.runningQueries[queryId].aggreg = this.runningQueries[queryId].aggreg.concat(aggregationData);
		}
		if(this.runningQueries[queryId].currentNumObjects === this.runningQueries[queryId].maxObjects){
			//Have to terminate
			this.returnInformation(queryId, this.runningQueries[queryId].queryCallback);
		}
		else if(this.runningQueries[queryId].maxObjects !== -1){
			//Restart timer
			this.objectCountStopper(queryId, this.runningQueries[queryId].currentNumObjects, this.runningQueries[queryId].queryCallback);
		}
	}
};

QueryExecutor.prototype.registerQuery = function(maxObjects, timeOut, queryCallback, idCallback) {
	var that = this;
	var randomNumber = Math.floor((Math.random() * 100) + 1);
	var currentTime = this.date.getTime();

	var queryId = randomNumber + "-" + currentTime;
	this.runningQueries[queryId] = {};
	this.runningQueries[queryId].data = [];
	this.runningQueries[queryId].aggreg = [];
	this.runningQueries[queryId].maxObjects = -1;
	this.runningQueries[queryId].currentNumObjects = 0;
	this.runningQueries[queryId].queryCallback = queryCallback;
	if (maxObjects !== undefined) {
		this.runningQueries[queryId].maxObjects = maxObjects;
		this.objectCountStopper(queryId, 0, queryCallback);
	}
	if (timeOut !== undefined) {
		// Setup the timeout handler so at the end to inform the client
		setTimeout(function() {
			that.returnInformation(queryId, queryCallback);
		}, timeOut * 1000);
	}
	if (maxObjects !== undefined && timeOut !== undefined) {
		// Setup the timeout handler so at the end to inform the client default
		// value of 5 sec
		setTimeout(function() {
			that.returnInformation(queryId, queryCallback);
		}, 5000);
	}
	idCallback(undefined, queryId);
};

QueryExecutor.prototype.updateWithLocalMatch = function(queryId, selectProperties, namespace, filterPlan) {
	var that = this;
	this.propertiesProcessor(selectProperties, function(processedProperty) {
		var localData = that.storageApis.lscan(namespace);
		that.filterBuilder.createFilterFromWhereClause(filterPlan, function(err, dataFilter) {
			var filteredData = localData.filter(dataFilter);
			if (filteredData.length > 0) {
				var processedFilteredData = [];
				filteredData.map(function(currentData) {
					var data = [];
					if (processedProperty.prop.length !== 0) {
						processedProperty.prop.forEach(function(prop) {
							data[prop] = currentData[prop];
						});
					} else {
						data = currentData;
					}

					processedFilteredData = processedFilteredData.concat(data);
				});

				that.updateQueryHolderInformation(queryId, processedFilteredData, processedProperty.aggreg);
			}
		});
	});
};

QueryExecutor.prototype.returnInformation = function(queryId, callback) {
	var queryData = this.runningQueries[queryId];
	delete this.runningQueries[queryId];

	var data = queryData.data;

	// TODO process data for aggregation
	if (queryData.aggreg.length !== 0) {
		console.log("TODO aggregation processing")
	}
	callback(undefined, data);
};

// Utility methods
QueryExecutor.prototype.objectCountStopper = function(queryId, prevObjCount, queryCallback) {
	var that = this;
	setTimeout(function() {
		if (that.runningQueries[queryId] !== undefined
				&& that.runningQueries[queryId].currentNumObjects === prevObjCount)
			that.returnInformation(queryId, queryCallback);
	}, 2000);
};

QueryExecutor.prototype.MAX_AGGREGATION = 'MAX';
QueryExecutor.prototype.MIN_AGGREGATION = 'MIN';
QueryExecutor.prototype.COUNT_AGGREGATION = 'COUNT';
QueryExecutor.prototype.SUM_AGGREGATION = 'SUM';
QueryExecutor.prototype.AVR_AGGREGATION = 'AVR';

QueryExecutor.prototype.propertiesProcessor = function(selectProperties, callback) {
	var that = this;
	var normalize = function(prop, begining, end) {
		return prop.slice(begining, end);
	};
	var prepareFuncReturnForAgg = function(prop, aggregation) {
		return {
			'prop' : [ prop ],
			'aggreg' : [ {
				'typ' : aggregation,
				'prop' : prop
			} ]
		};
	};
	var processedProperty = {
		'prop' : [],
		'aggreg' : []
	};

	if (selectProperties.indexOf('*') == -1) {
		processedProperty = selectProperties.map(function(element) {
			var splitedEl = element;
			switch (splitedEl[0]) {
			case that.MAX_AGGREGATION:
				var normalized = normalize(element, that.MAX_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, that.MAX_AGGREGATION);
				break;
			case that.MIN_AGGREGATION:
				var normalized = normalize(element, that.MIN_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, that.MIN_AGGREGATION);
				break;
			case that.COUNT_AGGREGATION:
				var normalized = normalize(element, that.COUNT_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, that.COUNT_AGGREGATION);
				break;
			case that.SUM_AGGREGATION:
				var normalized = normalize(element, that.SUM_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, that.SUM_AGGREGATION);
				break;
			case that.AVR_AGGREGATION:
				var normalized = normalize(element, that.AVR_AGGREGATION.length + 1, element.length - 1);
				return prepareFuncReturnForAgg(normalized, that.AVR_AGGREGATION);
				break;
			default:
				return {
					'prop' : [ element ],
					'aggreg' : []
				};
				break;
			}
		}).reduce(function(previousValue, currentValue, index, array) {
			return {
				'prop' : previousValue.prop.concat(currentValue.prop),
				'aggreg' : previousValue.aggreg.concat(currentValue.aggreg)
			};
		}, processedProperty);
	}
	callback(processedProperty);
};

module.exports.QueryExecutor = QueryExecutor;