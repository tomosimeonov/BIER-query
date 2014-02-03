/**
 * Runner to process sql plans for simple select operation (no joining).
 */
var joinBuilderFactory = require('../builders/JoinQueryConfigurationBuilder');

function JoinSelectSQLPlanRunner(joinExecutor){
	var self = this instanceof JoinSelectSQLPlanRunner ? this : Object.create(JoinSelectSQLPlanRunner.prototype);
	self.joinExecutor = joinExecutor;
	return self;
};

JoinSelectSQLPlanRunner.prototype.executePlan = function(sqlPlan, emiter) {
	var that = this;
	_createJoinConfiguration(sqlPlan, this, function(err, config) {
		if (err) {
			_emitError(emiter, err);
		} else {
			that.joinExecutor.executeQuery(config, emiter);
		}
	});
};

//Utility functions
var _createJoinConfiguration = function(plan, context, callback) {
	var builder = joinBuilderFactory.JoinQueryConfigurationBuilder();

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

	if (join === undefined || join.cond.operator === undefined || join.cond.operator !== "=")
		callback(new Error("Not recognized join syntaxis"), undefined);
	else {
		var left = join.cond.left.split('.');
		var right = join.cond.right.split('.');

		builder = builder.setNamespaceOne(plan.FROM[0]).setNamespaceTwo(join.table).setFullType(type);
		if (left[0] === plan.FROM[0]) {
			builder = builder.setJoinPropertyOne(left[1]).setJoinPropertyTwo(right[1]);
		} else {
			builder = builder.setJoinPropertyOne(right[1]).setJoinPropertyTwo(left[1]);
		}
		// TODO make the time extraction correct
		builder = builder.setFilterPlan(plan.WHERE);

		if (plan.TIMEOUT && plan.TIMEOUT.time !== undefined) {
			builder.setTimeout(plan.TIMEOUT.time);
		}
		if (plan.TIMEOUT && plan.TIMEOUT.objects !== undefined) {
			builder.setMaxObjects(plan.TIMEOUT.objects);
		}
		
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