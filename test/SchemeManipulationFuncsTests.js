/**
 * Testing the query scheme functionality
 */
var schemeQueryAPi = require('../lib/SchemeManipulationFuncs');
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

var realScheme = [];
realScheme.table = tableName;
realScheme.properties = [];
realScheme.properties.id = {
	type : 'int',
	notNull : true
};
realScheme.primaryKey = 'id';
realScheme.indexes = [];

realScheme.properties.fname = {
	type : 'string',
	notNull : false
};
realScheme.properties.lname = {
	type : 'string',
	notNull : false
};

var initialScheme = [];
initialScheme.table = tableName;
initialScheme.definitions = [];
initialScheme.definitions[0] = columnId;
initialScheme.definitions[1] = columnFirstName;
initialScheme.definitions[2] = columnLastName;
initialScheme.definitions[3] = primaryKeyProperty;

exports.schemeTransformation = {
	shouldTransformIntialPlanToRealOne : function(test) {
		schemeQueryAPi.permenentSchemeCreateror(initialScheme, function(err, scheme) {
			test.equal(err, undefined, "Should not produce error");
			test.deepEqual(realScheme, scheme, "Scheme did not match");
			test.done();
		});
	},

	shouldCrashWhenNoSchemeIsProvided : function(test) {
		schemeQueryAPi.permenentSchemeCreateror(undefined, function(err, scheme) {
			test.notEqual(err, undefined, "Should throw error if scheme is not defined.");
			test.equal(undefined, scheme, "Should not produce real scheme.");
			test.done();
		});
	},

	shouldNotCrashWhenNameSpaceIsMissing : function(test) {
		var tempInitScheme = extend([], initialScheme);
		tempInitScheme.table = undefined;

		var tempRealScheme = extend([], realScheme);
		tempRealScheme.table = undefined;

		schemeQueryAPi.permenentSchemeCreateror(tempInitScheme, function(err, scheme) {
			test.equal(err, undefined, "Should not throw error about missing namespace.");
			test.deepEqual(tempRealScheme, scheme, "Should produce scheme without namespace.");
			test.done();
		});
	},

	shouldNotCrashWhenThereAreNoProperties : function(test) {
		var tempInitScheme = extend([], initialScheme);
		tempInitScheme.definitions = [];

		var tempRealScheme = extend([], realScheme);
		tempRealScheme.properties = [];
		delete tempRealScheme.primaryKey;

		schemeQueryAPi.permenentSchemeCreateror(tempInitScheme, function(err, scheme) {
			test.equal(err, undefined, "Should not throw error about missing properties.");
			test.deepEqual(tempRealScheme, scheme, "Should produce scheme without properties.");
			test.done();
		});
	},

	shouldCrashWhenThereIsNoPropertiesElement : function(test) {
		var tempInitScheme = extend([], initialScheme);
		delete tempInitScheme.definitions;

		schemeQueryAPi.permenentSchemeCreateror(tempInitScheme, function(err, scheme) {
			test.notEqual(err, undefined, "Should throw error about missing properties.");
			test.equal(undefined, scheme, "Should not produce scheme without properties.");
			test.done();
		});
	}
};

exports.schemeValidation = {
	shouldReturnErrorWhenSchemeDoesNotHaveNamespace : function(test) {
		var tempRealScheme = extend([], realScheme);
		tempRealScheme.table = undefined;

		schemeQueryAPi.isSatisfingSchemeStructure(tempRealScheme, function(err, correct) {
			test.deepEqual(err, new Error(("No namespace provided.")), "Should throw error about missing namespace.");
			test.equal(false, correct, "Should not validate scheme.");

			tempRealScheme.table = "";
			schemeQueryAPi.isSatisfingSchemeStructure(tempRealScheme, function(err, correct) {
				test.deepEqual(err, new Error(("No scheme provided.")), "Should throw error about missing namespace.");
				test.equal(false, correct, "Should not validate scheme.");
				test.done();
			});
		});
	},

	shouldReturnErrorWhenSchemeDoesNotHaveProperties : function(test) {
		var tempRealScheme = extend([], realScheme);
		tempRealScheme.properties = undefined;

		schemeQueryAPi.isSatisfingSchemeStructure(tempRealScheme,
				function(err, correct) {
					test.deepEqual(err, new Error(("No properties provided.")),
							"Should throw error about missing properties.");
					test.equal(false, correct, "Should not validate scheme.");

					tempRealScheme.properties = [];
					schemeQueryAPi.isSatisfingSchemeStructure(tempRealScheme, function(err, correct) {
						test.deepEqual(err, new Error(("No properties provided.")),
								"Should throw error about missing properties.");
						test.equal(false, correct, "Should not validate scheme.");
						test.done();
					});
				});
	},

	shouldReturnErrorWhenSchemeDoesHavePropertiesWithWrongFormat : function(test) {
		var tempRealScheme = extend([], realScheme);
		tempRealScheme.properties.wrong = {};
		schemeQueryAPi.isSatisfingSchemeStructure(tempRealScheme,
				function(err, correct) {
					test.deepEqual(err, new Error("Property with wrong format."),
							"Should throw error about wrong properties.");
					test.equal(false, correct, "Should not validate scheme.");
					delete tempRealScheme.properties.wrong;
					test.done();
				});
	},

	shouldReturnErrorWhenSchemeDoesNotHavePrimaryKey : function(test) {
		var tempRealScheme = extend([], realScheme);
		tempRealScheme.primaryKey = undefined;
		schemeQueryAPi.isSatisfingSchemeStructure(tempRealScheme, function(err, correct) {

			test.deepEqual(err, new Error("No primary key or primary key not specified as property"),
					"Should throw error about missing primary key.");
			test.equal(false, correct, "Should not validate scheme.");

			tempRealScheme.primaryKey = "";
			schemeQueryAPi.isSatisfingSchemeStructure(tempRealScheme, function(err, correct) {

				test.deepEqual(err, new Error("No primary key or primary key not specified as property"),
						"Should throw error about missing  primary key.");
				test.equal(false, correct, "Should not validate scheme.");

				tempRealScheme.primaryKey = "no_id";
				schemeQueryAPi.isSatisfingSchemeStructure(tempRealScheme, function(err, correct) {

					test.deepEqual(err, new Error("No primary key or primary key not specified as property"),
							"Should throw error about missing  primary key.");
					test.equal(false, correct, "Should not validate scheme.");
					test.done();
				});
			});
		});
	},

	shouldReturnErrorWhenSchemeDoesNotHaveIndex : function(test) {
		var tempRealScheme = extend([], realScheme);
		delete tempRealScheme.indexes;
		schemeQueryAPi.isSatisfingSchemeStructure(tempRealScheme, function(err, correct) {
			test.deepEqual(err, new Error("Index element should be present even if there are no indexes"),
					"Should throw error about missing indexes.");
			test.equal(false, correct, "Should not validate scheme.");
			test.done();
		});
	},

	shouldValidateSchemeWhenItIsCorrect : function(test) {
		schemeQueryAPi.isSatisfingSchemeStructure(realScheme, function(err, correct) {
			test.equal(err, undefined, "Should not throw error on correct scheme.");
			test.equal(true, correct, "Should validate scheme.");
			test.done();
		});
	}
};

exports.isSchemeValid = {

	shouldValidatePropertiesAsSchemeComplingWhenTheyAreInTheScheme : function(test) {
		var properties = [ 'id', 'fname' ];
		schemeQueryAPi.isSatisfingScheme(properties, realScheme, function(err, correct) {
			test.equal(err, undefined, "Should not throw error when is scheme compling.");
			test.equal(true, correct, "Properties are correct, should match");
			test.done();
		});
	},
	
	shouldNotNValidatePropertiesAsSchemeComplingWhenThereIsPropNotInScheme : function(test) {
		var properties = [ 'id', 'fname1' ];
		schemeQueryAPi.isSatisfingScheme(properties, realScheme, function(err, correct) {
			test.equal(err, undefined, "Should not throw error when is scheme compling.");
			test.equal(false, correct, "Properties are correct, should match");
			test.done();
		});
	},
	
	shouldProduceAnErrorWhenFormatIsNotAsExpected : function(test) {
		var properties = undefined;
		schemeQueryAPi.isSatisfingScheme(properties, realScheme, function(err, correct) {
			test.notEqual(err, undefined, "Should throw error when when provide undentified as props");
			//TODO Should be false
			test.equal(true, correct, "Properties are correct, should match");
			
			properties = [];
			var tempRealScheme = extend([], realScheme);
			delete tempRealScheme.properties;
			schemeQueryAPi.isSatisfingScheme(properties, tempRealScheme, function(err, correct) {
				test.notEqual(err, undefined, "Should throw error when scheme wrong format");
				//TODO Should be false
				test.equal(true, correct, "Properties are not correct, should not match");
				test.done();
			});
		});
	}
};