/**
 * Statistic holder object.
 */
var curry = require('curry');

function StatisticHolder() {
	var self = this instanceof StatisticHolder ? this : Object.create(StatisticHolder.prototype);

	// Informations
	self.numberOfQueries = 0;
	self.localPerformanceHolder = [];
	self.localPerformanceHolderIndex = -1;

	self.logLevel = 2;
	// Listeners for statistic changes
	self.listeners = [];
	return self;
}

StatisticHolder.prototype.NEW_QUERY = "NEW_QUERY";
StatisticHolder.prototype.FIN_QUERY = "QUERY_FINISHED";
StatisticHolder.prototype.QUERY_PERF = "PERFORMANCE";
StatisticHolder.prototype.LOG_FINE = "LOG_FINE";
StatisticHolder.prototype.LOG_FINER = "LOG_FINER";

StatisticHolder.prototype.emitEvent = function(tag, data) {
	switch (tag) {
	case this.NEW_QUERY:
		this.numberOfQueries = this.numberOfQueries + 1;
		this.informListeners(this.NEW_QUERY);
		break;
	case this.FIN_QUERY:
		this.numberOfQueries = this.numberOfQueries - 1;
		this.informListeners(this.FIN_QUERY);
		break;
	case this.QUERY_PERF:
		this.localPerformanceHolderIndex = (this.localPerformanceHolderIndex + 1) % 10;
		this.localPerformanceHolder[this.localPerformanceHolderIndex] = data;
		this.informListeners(this.QUERY_PERF);
		break;
	case this.LOG_FINE:
		this.informListeners(this.LOG_FINE,data);
		break;
	case this.LOG_FINER:
		this.informListeners(this.LOG_FINER,data);
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
		emitter.emit("RUNNING_QUERIES", numberOfQueries);
	});
	var performance = curry(function(everageRunTime, emitter) {
		emitter.emit("PERFORMANCE", everageRunTime);
	});
	var log = curry(function(logLevel, text, emitter) {
		emitter.emit(logLevel, text);
	});

	var execute = function(){};
	
	switch (tag) {
	case this.NEW_QUERY:
		execute = runningQueries(this.numberOfQueries);
		break;
	case this.FIN_QUERY:
		execute = runningQueries(this.numberOfQueries);
		break;
	case this.QUERY_PERF:
		var performanceL = this.averageRunningTime();
		execute = performance(performanceL);
		break;
	case this.LOG_FINE:
		execute = log(this.LOG_FINE,data);
		break;
	case this.LOG_FINER:
		execute = log(this.LOG_FINER,data);
		break;
	}

	this.listeners.forEach(execute);
};

StatisticHolder.prototype.addListener = function(listener) {
	this.listeners.push(listener);
};

module.exports.StatisticHolder = StatisticHolder;