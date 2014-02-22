/**
 * Runner to process sql plans for insert operation.
 * @author Tomo Simeonov
 */
var schemeFunctions = require('../SchemeManipulationFuncs');
var utilities = require('../EmitterUtilities');
var constants = require('../Constants');

function SchemeInsertSQLPlanRunner(store,eventHolder){
	var self = this instanceof SchemeInsertSQLPlanRunner ? this : Object.create(SchemeInsertSQLPlanRunner.prototype);
	self.store = store;
	self.eventHolder = eventHolder;
	self.counter = 0;
	return self;
};

/**
 * Main entry point for executing SQL statements inserting schemes in the database.
 */
SchemeInsertSQLPlanRunner.prototype.executePlan = function(sqlPlan, emiter) {
	var that = this;
	var queryId = "sch-ins-" + this.counter;
	this.counter++;

	utilities.emitExecuting(emiter, queryId);
	this.eventHolder.emitEvent(constants.LOG_INFO, [ queryId, "Transforming new scheme into BIER scheme format" ]);
	var namespace = sqlPlan.table;
	schemeFunctions.permenentSchemeCreateror(sqlPlan, function(err, realScheme) {
		if (err)
			utilities.emitError(emiter, err);
		else
			that.eventHolder.emitEvent(constants.LOG_FINE, [ queryId, "Checking if the transformed scheme is satisfing the internal format" ]);
			schemeFunctions.isSatisfingSchemeStructure(realScheme, function(err, correctness) {
				if (err)
					utilities.emitError(emiter, err);
				else {
					if(correctness){
						that.eventHolder.emitEvent(constants.LOG_FINE, [ queryId, "Scheme strucure passing tests."]);
						that.store(namespace,realScheme);
						utilities.emitSuccess(emiter, "Scheme for namespace " + namespace + " has been inserted in the database.");
						utilities.emitQueryFinish(emiter);
					}
					else{
						utilities.emitError(emiter, new Error("Cannot transform to internal correct format."));
						utilities.emitQueryFinish(emiter);
					}
				}
			});
	});
};

module.exports.SchemeInsertSQLPlanRunner = SchemeInsertSQLPlanRunner;