/**
 * Testing the query schema functionality
 */
var schemaQueryAPi = require('../lib/SchemaQueryAPI');
var extend = require('util')._extend;

var tableName = 'persons';

var columnId = {
	type : 'INT',
	name : 'id',
	notNull : true
};

var columnFirstName = {
	type : 'VARCHAR',
	name : 'fname'
};
var columnLastName = {
	type : 'VARCHAR',
	name : 'lname',
	notNull : false
};
var primaryKeyProperty = {
	type : 'CONSTRAINT',
	constraint : 'PRIMARY KEY',
	constraintName : 'pk_PersonID',
	columns : [ 'id' ]
};

var realSchema = [];
realSchema.table = tableName;
realSchema.primaryKey = 'id';
realSchema.indexes = [];
realSchema.properties = [];
realSchema.properties.id = {
	type : 'INT',
	notNull : true
};
realSchema.properties.fname = {
	type : 'VARCHAR',
	notNull : false
};
realSchema.properties.lname = {
	type : 'VARCHAR',
	notNull : false
};

var initialSchema = [];
initialSchema.table = tableName;
initialSchema.definitions = [];
initialSchema.definitions[0] = columnId;
initialSchema.definitions[1] = columnFirstName;
initialSchema.definitions[2] = columnLastName;
initialSchema.definitions[3] = primaryKeyProperty;

exports.schemaTransformation = {
	shouldTransformIntialPlanToRealOne : function(test) {
		schemaQueryAPi.permenentSchemaCreateror(initialSchema, function(err, schema) {
			test.equal(err, undefined, "Should not produce error");
			test.deepEqual(realSchema, schema, "Schema did not match");
			test.done();
		});
	},

	shouldCrashWhenNoSchemaIsProvided : function(test) {
		schemaQueryAPi.permenentSchemaCreateror(undefined, function(err, schema) {
			test.notEqual(err, undefined, "Should throw error if schema is not defined.");
			test.equal(undefined, schema, "Should not produce real schema.");
			test.done();
		});
	},

	shouldNotCrashWhenNameSpaceIsMissing : function(test) {
		var tempInitSchema = extend([], initialSchema);
		tempInitSchema.table = undefined;

		var tempRealSchema = extend([], realSchema);
		tempRealSchema.table = undefined;

		schemaQueryAPi.permenentSchemaCreateror(tempInitSchema, function(err, schema) {
			test.equal(err, undefined, "Should not throw error about missing namespace.");
			test.deepEqual(tempRealSchema, schema, "Should produce schema without namespace.");
			test.done();
		});
	},

	shouldNotCrashWhenThereAreNoProperties : function(test) {
		var tempInitSchema = extend([], initialSchema);
		tempInitSchema.definitions = [];

		var tempRealSchema = extend([], realSchema);
		tempRealSchema.properties = [];
		delete tempRealSchema.primaryKey;

		schemaQueryAPi.permenentSchemaCreateror(tempInitSchema, function(err, schema) {
			test.equal(err, undefined, "Should not throw error about missing properties.");
			test.deepEqual(tempRealSchema, schema, "Should produce schema without properties.");
			test.done();
		});
	},

	shouldCrashWhenThereIsNoPropertiesElement : function(test) {
		var tempInitSchema = extend([], initialSchema);
		delete tempInitSchema.definitions;

		schemaQueryAPi.permenentSchemaCreateror(tempInitSchema, function(err, schema) {
			test.notEqual(err, undefined, "Should throw error about missing properties.");
			test.equal(undefined, schema, "Should not produce schema without properties.");
			test.done();
		});
	}
};

exports.schemaValidation = {
	shouldReturnErrorWhenSchemaDoesNotHaveNamespace : function(test) {
		var tempRealSchema = extend([], realSchema);
		tempRealSchema.table = undefined;

		schemaQueryAPi.isSchemaStructureCorrect(tempRealSchema, function(err, correct) {
			test.deepEqual(err, new Error(("No namespace provided.")), "Should throw error about missing namespace.");
			test.equal(false, correct, "Should not validate schema.");

			tempRealSchema.table = "";
			schemaQueryAPi.isSchemaStructureCorrect(tempRealSchema, function(err, correct) {
				test.deepEqual(err, new Error(("No schema provided.")), "Should throw error about missing namespace.");
				test.equal(false, correct, "Should not validate schema.");
				test.done();
			});
		});
	},

	shouldReturnErrorWhenSchemaDoesNotHaveProperties : function(test) {
		var tempRealSchema = extend([], realSchema);
		tempRealSchema.properties = undefined;

		schemaQueryAPi.isSchemaStructureCorrect(tempRealSchema,
				function(err, correct) {
					test.deepEqual(err, new Error(("No properties provided.")),
							"Should throw error about missing properties.");
					test.equal(false, correct, "Should not validate schema.");

					tempRealSchema.properties = [];
					schemaQueryAPi.isSchemaStructureCorrect(tempRealSchema, function(err, correct) {
						test.deepEqual(err, new Error(("No properties provided.")),
								"Should throw error about missing properties.");
						test.equal(false, correct, "Should not validate schema.");
						test.done();
					});
				});
	},

	shouldReturnErrorWhenSchemaDoesHavePropertiesWithWrongFormat : function(test) {
		var tempRealSchema = extend([], realSchema);
		tempRealSchema.properties.wrong = {};
		schemaQueryAPi.isSchemaStructureCorrect(tempRealSchema,
				function(err, correct) {
					test.deepEqual(err, new Error("Property with wrong format."),
							"Should throw error about wrong properties.");
					test.equal(false, correct, "Should not validate schema.");
					delete tempRealSchema.properties.wrong;
					test.done();
				});
	},

	shouldReturnErrorWhenSchemaDoesNotHavePrimaryKey : function(test) {
		var tempRealSchema = extend([], realSchema);
		tempRealSchema.primaryKey = undefined;
		schemaQueryAPi.isSchemaStructureCorrect(tempRealSchema, function(err, correct) {

			test.deepEqual(err, new Error("No primary key or primary key not specified as property"),
					"Should throw error about missing primary key.");
			test.equal(false, correct, "Should not validate schema.");

			tempRealSchema.primaryKey = "";
			schemaQueryAPi.isSchemaStructureCorrect(tempRealSchema, function(err, correct) {

				test.deepEqual(err, new Error("No primary key or primary key not specified as property"),
						"Should throw error about missing  primary key.");
				test.equal(false, correct, "Should not validate schema.");

				tempRealSchema.primaryKey = "no_id";
				schemaQueryAPi.isSchemaStructureCorrect(tempRealSchema, function(err, correct) {

					test.deepEqual(err, new Error("No primary key or primary key not specified as property"),
							"Should throw error about missing  primary key.");
					test.equal(false, correct, "Should not validate schema.");
					test.done();
				});
			});
		});
	},

	shouldReturnErrorWhenSchemaDoesNotHaveIndex : function(test) {
		var tempRealSchema = extend([], realSchema);
		delete tempRealSchema.indexes;
		schemaQueryAPi.isSchemaStructureCorrect(tempRealSchema, function(err, correct) {
			test.deepEqual(err, new Error("Index element should be present even if there are no indexes"),
					"Should throw error about missing indexes.");
			test.equal(false, correct, "Should not validate schema.");
			test.done();
		});
	},

	shouldValidateSchemaWhenItIsCorrect : function(test) {
		schemaQueryAPi.isSchemaStructureCorrect(realSchema, function(err, correct) {
			test.equal(err, undefined, "Should not throw error on correct schema.");
			test.equal(true, correct, "Should validate schema.");
			test.done();
		});
	}
};

exports.isSchemaValid = {

	shouldValidatePropertiesAsSchemaComplingWhenTheyAreInTheSchema : function(test) {
		var properties = [ 'id', 'fname' ];
		schemaQueryAPi.isSchemaComplied(properties, realSchema, function(err, correct) {
			test.equal(err, undefined, "Should not throw error when is schema compling.");
			test.equal(true, correct, "Properties are correct, should match");
			test.done();
		});
	},
	
	shouldNotNValidatePropertiesAsSchemaComplingWhenThereIsPropNotInSchema : function(test) {
		var properties = [ 'id', 'fname1' ];
		schemaQueryAPi.isSchemaComplied(properties, realSchema, function(err, correct) {
			test.equal(err, undefined, "Should not throw error when is schema compling.");
			test.equal(false, correct, "Properties are correct, should match");
			test.done();
		});
	},
	
	shouldProduceAnErrorWhenFormatIsNotAsExpected : function(test) {
		var properties = undefined;
		schemaQueryAPi.isSchemaComplied(properties, realSchema, function(err, correct) {
			test.notEqual(err, undefined, "Should throw error when when provide undentified as props");
			//TODO Should be false
			test.equal(true, correct, "Properties are correct, should match");
			
			properties = [];
			var tempRealSchema = extend([], realSchema);
			delete tempRealSchema.properties;
			schemaQueryAPi.isSchemaComplied(properties, tempRealSchema, function(err, correct) {
				test.notEqual(err, undefined, "Should throw error when schema wrong format");
				//TODO Should be false
				test.equal(true, correct, "Properties are not correct, should not match");
				test.done();
			});
		});
	}
};