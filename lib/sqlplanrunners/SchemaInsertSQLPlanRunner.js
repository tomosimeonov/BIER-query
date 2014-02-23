/**
 * Runner to process sql plans for insert operation.
 * 
 * @author Tomo Simeonov
 */
var schemaFunctions = require('../SchemaManipulationFuncs');
var utilities = require('../EmitterUtilities');
var constants = require('../Constants');

function SchemaInsertSQLPlanRunner(store, eventHolder) {
	var self = this instanceof SchemaInsertSQLPlanRunner ? this : Object.create(SchemaInsertSQLPlanRunner.prototype);
	self.store = store;
	self.eventHolder = eventHolder;
	self.counter = 0;
	return self;
};

/**
 * Main entry point for executing SQL statements inserting schemas in the
 * database.
 */
SchemaInsertSQLPlanRunner.prototype.executePlan = function(sqlPlan, emiter) {
	var that = this;
	var queryId = "sch-ins-" + this.counter;
	this.counter++;

	utilities.emitExecuting(emiter, queryId);
	this.eventHolder.emitEvent(constants.LOG_INFO, [ queryId, "Transforming new schema into BIER schema format" ]);

	var namespace = sqlPlan.table;

	schemaFunctions.permenentSchemaCreateror(sqlPlan, function(err, realSchema) {
		if (err)
			utilities.emitError(emiter, err);
		else
			that.eventHolder.emitEvent(constants.LOG_FINE, [ queryId,
					"Checking if the transformed schema is satisfing the internal format" ]);

		schemaFunctions.isSatisfingSchemaStructure(realSchema, function(err, correctness) {
			if (err)
				utilities.emitError(emiter, err);
			else {
				if (correctness) {
					that.eventHolder.emitEvent(constants.LOG_FINER, [ queryId, "Schema strucure passing tests." ]);
					that.store(namespace, realSchema);
					utilities.emitSuccess(emiter, "Schema for namespace " + namespace
							+ " has been inserted in the database.");
					utilities.emitQueryFinish(emiter);
				} else {
					that.eventHolder
							.emitEvent(constants.LOG_FINER, [ queryId, "Schema strucure does not pass tests." ]);
					utilities.emitError(emiter, new Error("Cannot transform to internal correct format."));
					utilities.emitQueryFinish(emiter);
				}
			}
		});
	});
};

module.exports.SchemaInsertSQLPlanRunner = SchemaInsertSQLPlanRunner;