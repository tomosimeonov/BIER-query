/**
 * Runner to process sql plans for simple select operation (no joining).
 * 
 * @author Tomo Simeonov
 */
var joinBuilderFactory = require('../builders/JoinQueryConfigurationBuilder');
var utilities = require('../EmitterUtilities');

function JoinSelectSQLPlanRunner(joinExecutor) {
	var self = this instanceof JoinSelectSQLPlanRunner ? this : Object.create(JoinSelectSQLPlanRunner.prototype);
	self.joinExecutor = joinExecutor;
	return self;
};

/**
 * Executes the plan for join statement.
 * 
 * @param sqlPlan
 * @param emiter
 */
JoinSelectSQLPlanRunner.prototype.executePlan = function(sqlPlan, emiter) {
	var that = this;
	_createJoinConfiguration(sqlPlan, this, function(err, config) {
		if (err) {
			utilities.emitError(emiter, err);
		} else {
			that.joinExecutor.executeQuery(config, emiter);
		}
	});
};

// Utility functions
var _createJoinConfiguration = function(plan, context, callback) {
	var builder = joinBuilderFactory.JoinQueryConfigurationBuilder();

	// Finds type of join
	var join = plan['LEFT JOIN'];
	var type = builder.ONE;

	if (join === undefined) {
		join = plan['INNER JOIN'];
		type = builder.NONE;
	}
	if (join === undefined) {
		join = plan['RIGHT JOIN'];
		type = builder.TWO;
	}

	// Check if type recognizable
	if (join === undefined || join.cond.operator === undefined || join.cond.operator !== "=")
		callback(new Error("Not recognized join syntaxis, use only single equality join"), undefined);
	else {

		// Set namespaces, prop joins
		var left = join.cond.left.split('.');
		var right = join.cond.right.split('.');

		builder = builder.setNamespaceOne(plan.FROM[0]).setNamespaceTwo(join.table).setFullType(type);

		if (left[0] === plan.FROM[0]) {
			builder = builder.setJoinPropertyOne(left[1]).setJoinPropertyTwo(right[1]);
		} else {
			builder = builder.setJoinPropertyOne(right[1]).setJoinPropertyTwo(left[1]);
		}

		// Set filter plan
		builder = builder.setFilterPlan(plan.WHERE);

		// Set timeout
		if (plan.TIMEOUT && plan.TIMEOUT.time !== undefined) {
			builder.setTimeout(plan.TIMEOUT.time);
		}
		if (plan.TIMEOUT && plan.TIMEOUT.objects !== undefined) {
			builder.setMaxObjects(plan.TIMEOUT.objects);
		}

		// Processed the select part to one recognizable by the executor
		context.joinExecutor.formatSelectProperties(plan.SELECT, function(err, props) {
			if (err) {
				callback(err, undefined);
			} else {
				callback(undefined, builder.setFormattedProperties(props).buildQueryConfig());
			}

		});
	}

};

module.exports.JoinSelectSQLPlanRunner = JoinSelectSQLPlanRunner;