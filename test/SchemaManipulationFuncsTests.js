/**
 * Testing the query schema functionality
 */
var schemaQueryAPi = require('../lib/SchemaManipulationFuncs');
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
realSchema.properties = [];
realSchema.properties.id = {
	type : 'int',
	notNull : true
};
realSchema.primaryKey = 'id';
realSchema.indexes = [];

realSchema.properties.fname = {
	type : 'string',
	notNull : false
};
realSchema.properties.lname = {
	type : 'string',
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
			console.log(err)
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

		schemaQueryAPi.isSatisfingSchemaStructure(tempRealSchema, function(err, correct) {
			test.deepEqual(err, new Error(("No namespace provided.")), "Should throw error about missing namespace.");
			test.equal(false, correct, "Should not validate schema.");

			tempRealSchema.table = "";
			schemaQueryAPi.isSatisfingSchemaStructure(tempRealSchema, function(err, correct) {
				test.deepEqual(err, new Error(("No schema provided.")), "Should throw error about missing namespace.");
				test.equal(false, correct, "Should not validate schema.");
				test.done();
			});
		});
	},

	shouldReturnErrorWhenSchemaDoesNotHaveProperties : function(test) {
		var tempRealSchema = extend([], realSchema);
		tempRealSchema.properties = undefined;

		schemaQueryAPi.isSatisfingSchemaStructure(tempRealSchema,
				function(err, correct) {
					test.deepEqual(err, new Error(("No properties provided.")),
							"Should throw error about missing properties.");
					test.equal(false, correct, "Should not validate schema.");

					tempRealSchema.properties = [];
					schemaQueryAPi.isSatisfingSchemaStructure(tempRealSchema, function(err, correct) {
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
		schemaQueryAPi.isSatisfingSchemaStructure(tempRealSchema,
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
		schemaQueryAPi.isSatisfingSchemaStructure(tempRealSchema, function(err, correct) {

			test.deepEqual(err, new Error("No primary key or primary key not specified as property"),
					"Should throw error about missing primary key.");
			test.equal(false, correct, "Should not validate schema.");

			tempRealSchema.primaryKey = "";
			schemaQueryAPi.isSatisfingSchemaStructure(tempRealSchema, function(err, correct) {

				test.deepEqual(err, new Error("No primary key or primary key not specified as property"),
						"Should throw error about missing  primary key.");
				test.equal(false, correct, "Should not validate schema.");

				tempRealSchema.primaryKey = "no_id";
				schemaQueryAPi.isSatisfingSchemaStructure(tempRealSchema, function(err, correct) {

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
		schemaQueryAPi.isSatisfingSchemaStructure(tempRealSchema, function(err, correct) {
			test.deepEqual(err, new Error("Index element should be present even if there are no indexes"),
					"Should throw error about missing indexes.");
			test.equal(false, correct, "Should not validate schema.");
			test.done();
		});
	},

	shouldValidateSchemaWhenItIsCorrect : function(test) {
		schemaQueryAPi.isSatisfingSchemaStructure(realSchema, function(err, correct) {
			test.equal(err, undefined, "Should not throw error on correct schema.");
			test.equal(true, correct, "Should validate schema.");
			test.done();
		});
	}
};

exports.isSchemaValid = {

	shouldValidatePropertiesAsSchemaComplingWhenTheyAreInTheSchema : function(test) {
		var properties = [ 'id', 'fname' ];
		schemaQueryAPi.isSatisfingSchema(properties, realSchema, function(err, correct) {
			test.equal(err, undefined, "Should not throw error when is schema compling.");
			test.equal(true, correct, "Properties are correct, should match");
			test.done();
		});
	},
	
	shouldNotNValidatePropertiesAsSchemaComplingWhenThereIsPropNotInSchema : function(test) {
		var properties = [ 'id', 'fname1' ];
		schemaQueryAPi.isSatisfingSchema(properties, realSchema, function(err, correct) {
			test.equal(err, undefined, "Should not throw error when is schema compling.");
			test.equal(false, correct, "Properties are correct, should match");
			test.done();
		});
	},
	
	shouldProduceAnErrorWhenFormatIsNotAsExpected : function(test) {
		var properties = undefined;
		schemaQueryAPi.isSatisfingSchema(properties, realSchema, function(err, correct) {
			test.notEqual(err, undefined, "Should throw error when when provide undentified as props");
			//TODO Should be false
			test.equal(true, correct, "Properties are correct, should match");
			
			properties = [];
			var tempRealSchema = extend([], realSchema);
			delete tempRealSchema.properties;
			schemaQueryAPi.isSatisfingSchema(properties, tempRealSchema, function(err, correct) {
				test.notEqual(err, undefined, "Should throw error when schema wrong format");
				//TODO Should be false
				test.equal(true, correct, "Properties are not correct, should not match");
				test.done();
			});
		});
	}
};


var schemaWithoutNumbers = [];
schemaWithoutNumbers.table = tableName;
schemaWithoutNumbers.properties = [];
schemaWithoutNumbers.properties.id = {
	type : 'string',
	notNull : true
};
schemaWithoutNumbers.primaryKey = 'id';
schemaWithoutNumbers.indexes = [];

schemaWithoutNumbers.properties.fname = {
	type : 'string',
	notNull : false
};
schemaWithoutNumbers.properties.lname = {
	type : 'string',
	notNull : false
};

var schemaWithNumbers = extend([], schemaWithoutNumbers);
schemaWithNumbers.properties.age = {
		type : 'int',
		notNull : false
	};

schemaWithNumbers.properties.salary = {
		type : 'float',
		notNull : false
	};
exports.transformStoredEntryToSchemaValid = {
		
		shouldTransformEntryWithoutNumbers : function(test){
			var storedEntry = {};
			storedEntry.id = "a";
			storedEntry.fname = "Tom";
			storedEntry.lname = "Sim";
			
			var properEntry = {};
			properEntry.id = "a";
			properEntry.fname = "Tom";
			properEntry.lname = "Sim";
			
			schemaQueryAPi.transformStoredEntryToSchemaValid(schemaWithoutNumbers,[storedEntry],function(entry){
				test.deepEqual([properEntry], entry, "There should be no change in the entry");
				test.done();
			});
		},
		shouldTransformEntryWithNumbers : function(test){
			var storedEntry = {};
			storedEntry.id = "a";
			storedEntry.fname = "Tom";
			storedEntry.lname = "Sim";
			storedEntry.age = "18";
			storedEntry.salary = "2.2";
			
			var properEntry = {};
			properEntry.id = "a";
			properEntry.fname = "Tom";
			properEntry.lname = "Sim";
			properEntry.age = 18;
			properEntry.salary = 2.2;
			
			schemaQueryAPi.transformStoredEntryToSchemaValid(schemaWithoutNumbers,[storedEntry],function(entry){
				test.deepEqual([properEntry], entry, "There should be change in the entry");
				test.done();
			});
		}
		
};