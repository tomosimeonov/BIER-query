/**
 * All type of logging is done through this object, anyone wanting to listen to
 * the events emitted have to register with it.
 */
var curry = require('curry');
var constants = require('./Constants');

function StatisticHolder() {
	var self = this instanceof StatisticHolder ? this : Object.create(StatisticHolder.prototype);

	// Informations
	self.numberOfQueries = 0;
	self.localPerformanceHolder = [];
	self.localPerformanceHolderIndex = -1;

	//Level of logging
	self.logLevel = 0;
	// Listeners for events
	self.listeners = [];
	return self;
}

StatisticHolder.prototype.emitEvent = function(tag, data) {
	switch (tag) {
	case constants.NEW_QUERY:
		this.numberOfQueries = this.numberOfQueries + 1;
		this.informListeners(constants.NEW_QUERY);
		break;
	case constants.FIN_QUERY:
		this.numberOfQueries = this.numberOfQueries - 1;
		this.informListeners(constants.FIN_QUERY);
		break;
	case constants.QUERY_PERF:
		this.localPerformanceHolderIndex = (this.localPerformanceHolderIndex + 1) % 10;
		this.localPerformanceHolder[this.localPerformanceHolderIndex] = data;
		this.informListeners(constants.QUERY_PERF);
		break;
	case constants.LOG_INFO:
		this.informListeners(constants.LOG_INFO, data);
		break;
	case constants.LOG_FINE:
		this.informListeners(constants.LOG_FINE, data);
		break;
	case constants.LOG_FINER:
		this.informListeners(constants.LOG_FINER, data);
		break;
	}

	this.informListeners();

};

StatisticHolder.prototype.setInfoLogLevel = function() {
	this.logLevel = 0;
};

StatisticHolder.prototype.setFineLogLevel = function() {
	this.logLevel = 1;
};

StatisticHolder.prototype.setFinerLogLevel = function() {
	this.logLevel = 2;
};

StatisticHolder.prototype.isInfoLogLevel = function() {
	return this.logLevel === 0;
};

StatisticHolder.prototype.isFineLogLevel = function() {
	return this.logLevel !== 0;
};

StatisticHolder.prototype.isFinerLogLevel = function() {
	return this.logLevel === 2;
};

StatisticHolder.prototype.averageRunningTime = function() {
	return this.localPerformanceHolder.reduce(function(prev, current) {
		return current + prev;
	}, 0) / this.localPerformanceHolder.length;
};

// Listener related code
StatisticHolder.prototype.informListeners = function(tag, data) {

	var runningQueries = curry(function(numberOfQueries, emitter) {
		emitter.emit(constants.RUNNING_QUERIES, numberOfQueries);
	});
	var performance = curry(function(everageRunTime, emitter) {
		emitter.emit(constants.QUERY_PERF, everageRunTime);
	});
	var log = curry(function(logLevel, text, emitter) {
		emitter.emit(logLevel, text);
	});

	var execute = function() {
	};
	var emitIt = true;

	switch (tag) {
	case constants.NEW_QUERY:
		execute = runningQueries(this.numberOfQueries);
		break;
	case constants.FIN_QUERY:
		execute = runningQueries(this.numberOfQueries);
		break;
	case constants.QUERY_PERF:
		var performanceL = this.averageRunningTime();
		execute = performance(performanceL);
		break;
	case constants.LOG_INFO:
		execute = log(this.LOG_INFO, data);
		break;
	case constants.LOG_FINE:
		execute = log(this.LOG_FINE, data);
		if (this.logLevel === 0)
			emitIt = false;
		break;
	case constants.LOG_FINER:
		execute = log(this.LOG_FINER, data);
		if (this.logLevel !== 2)
			emitIt = false;
		break;
	}

	if (emitIt) {
		this.listeners.forEach(execute);
	}

};

StatisticHolder.prototype.addListener = function(listener) {
	this.listeners.push(listener);
};

module.exports.StatisticHolder = StatisticHolder;