/**
 * Runner to process sql plans for simple select operation (no joining).
 * 
 * @author Tomo Simeonov
 */
var simpleBuilderFactory = require('../builders/SimpleQueryConfigurationBuilder');

function SimpleSelectSQLPlanRunner(simpleExecutor) {
	var self = this instanceof SimpleSelectSQLPlanRunner ? this : Object.create(SimpleSelectSQLPlanRunners.prototype);
	self.simpleExecutor = simpleExecutor;
	return self;
};

SimpleSelectSQLPlanRunner.prototype.executePlan = function(sqlPlan, emiter) {
	this.selectQuery(sqlPlan.SELECT, sqlPlan.FROM[0], sqlPlan.WHERE, sqlPlan.TIMEOUT, emiter);
};

SimpleSelectSQLPlanRunner.prototype.selectQuery = function(properties, namespace, filterPlan, timeout, emiter) {
	var that = this;
	// TODO Validate
	// TODO Index Search
	this.simpleExecutor.formatSelectProperties(properties, function(err, formatedSelectProperties) {

		// TODO extract from PHT
		var destinations = [];

		var builder = simpleBuilderFactory.SimpleQueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations(destinations).setFilterPlan(filterPlan)
				.setFormattedProperties(formatedSelectProperties);

		if (timeout && timeout.time !== undefined) {
			builder.setTimeout(timeout.time);
		}
		if (timeout && timeout.objects !== undefined) {
			builder.setMaxObjects(timeout.objects);
		}

		that.simpleExecutor.executeQuery(builder.buildQueryConfig(), emiter);
	});

};

module.exports.SimpleSelectSQLPlanRunner = SimpleSelectSQLPlanRunner;