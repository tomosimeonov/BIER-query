/**
 * New node file
 */
var utilities = require('../lib/executors/ExecutorUtilities');
var extend = require('util')._extend;

var schemaWithoutNumbers = {};

schemaWithoutNumbers.id = {
	type : 'string',
	notNull : true
};

schemaWithoutNumbers.fname = {
	type : 'string',
	notNull : false
};
schemaWithoutNumbers.lname = {
	type : 'string',
	notNull : false
};

var schemaWithNumbers = extend([], schemaWithoutNumbers);
schemaWithNumbers.age = {
	type : 'int',
	notNull : false
};

schemaWithNumbers.salary = {
	type : 'float',
	notNull : false
};
exports.transformStoredEntryToSchemaValid = {

	shouldTransformEntryWithoutNumbers : function(test) {
		var storedEntry = {};
		storedEntry.id = "a";
		storedEntry.fname = "Tom";
		storedEntry.lname = "Sim";

		var properEntry = {};
		properEntry.id = "a";
		properEntry.fname = "Tom";
		properEntry.lname = "Sim";

		utilities.transformStoredEntryToSchemaValid(schemaWithoutNumbers, [ storedEntry ], function(entry) {
			test.deepEqual([ properEntry ], entry, "There should be no change in the entry");
			test.done();
		});
	},
	shouldTransformEntryWithNumbers : function(test) {
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

		utilities.transformStoredEntryToSchemaValid(schemaWithoutNumbers, [ storedEntry ], function(entry) {
			test.deepEqual([ properEntry ], entry, "There should be change in the entry");
			test.done();
		});
	}

};