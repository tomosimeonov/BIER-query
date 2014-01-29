/**
 * Runner to process sql plans for insert operation.
 */
var schemaFunctions = require('../SchemaManipulationFuncs');
var utilities = require('../Utilities');

function SchemaInsertSQLPlanRunner(store){
	var self = this instanceof SchemaInsertSQLPlanRunner ? this : Object.create(SchemaInsertSQLPlanRunner.prototype);
	self.store = store;
	return self;
};

SchemaInsertSQLPlanRunner.prototype.executePlan = function(sqlPlan, emiter) {
	var namespace = sqlPlan.table;
	schemaFunctions.permenentSchemaCreateror(sqlPlan, function(err, realSchema) {
		if (err)
			utilities.emitError(emiter, err);
		else
			schemaFunctions.isSatisfingSchemaStructure(realSchema, function(err, correctness) {
				if (err)
					utilities.emitError(emiter, err);
				else {
					// TODO schema in DHT
					utilities.emitSuccess(emiter, "Schema for namespace " + namespace + " is been inserted in the database.");
				}
			});
	});
};

module.exports.SchemaInsertSQLPlanRunner = SchemaInsertSQLPlanRunner;