/**
 * Statistic holder object.
 */

function StatisticHolder() {
	var self = this instanceof StatisticHolder ? this : Object.create(StatisticHolder.prototype);

	// Informations
	self.numberOfQueries = 0;
	self.localPerformanceHolder = [];
	self.localPerformanceHolderIndex = -1;

	// Listeners for statistic changes
	self.listeners = [];
	return self;
}

StatisticHolder.prototype.NEW_QUERY = "n_q";
StatisticHolder.prototype.FIN_QUERY = "f_q";
StatisticHolder.prototype.QUERY_PERF = "l_p";

StatisticHolder.prototype.updateStatistics = function(tag, integer) {
	switch (tag) {
	case this.NEW_QUERY:
		this.numberOfQueries = this.numberOfQueries + 1;
		break;
	case this.FIN_QUERY:
		this.numberOfQueries = this.numberOfQueries - 1;
		break;
	case this.QUERY_PERF:
		this.localPerformanceHolderIndex = (this.localPerformanceHolderIndex + 1) % 10;
		this.localPerformanceHolder[this.localPerformanceHolderIndex] = integer;
		break;
	}

	this.informListeners();

};

StatisticHolder.prototype.averageRunningTime = function() {
	return this.localPerformanceHolder.reduce(function(prev, current) {
		return current + prev;
	}, 0) / this.localPerformanceHolder.length;
};

// Listener related code
StatisticHolder.prototype.informListeners = function() {
	this.listeners.forEach(function(elem) {
		elem();
	});
};

StatisticHolder.prototype.addListener = function(listener) {
	this.listeners.push(listener);
};

module.exports.StatisticHolder = StatisticHolder;