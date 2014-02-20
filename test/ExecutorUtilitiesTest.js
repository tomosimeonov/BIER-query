/**
 * New node file
 */
var utilities = require('../lib/executors/ExecutorUtilities');
var extend = require('util')._extend;

var schemaWithoutNumbers = {};
schemaWithoutNumbers.properties = [];
schemaWithoutNumbers.properties.id = {
	type : 'string',
	notNull : true
};

schemaWithoutNumbers.properties.fname = {
	type : 'string',
	notNull : false
};
schemaWithoutNumbers.properties.lname = {
	type : 'string',
	notNull : false
};

var schemaWithNumbers = {};
schemaWithNumbers.properties = [];
schemaWithNumbers.properties.age = {
	type : 'int',
	notNull : false
};

schemaWithNumbers.properties.salary = {
	type : 'float',
	notNull : false
};

schemaWithNumbers.properties.id = {
		type : 'string',
		notNull : true
	};

schemaWithNumbers.properties.fname = {
		type : 'string',
		notNull : false
	};
schemaWithNumbers.properties.lname = {
		type : 'string',
		notNull : false
	};
exports.produceUnmarshallerDataFunction = {

	shouldBeAbleToUnmarshallDataWithoutNumsOrAggreg : function(test) {
		var storedEntry = {};
		storedEntry.id = "a";
		storedEntry.fname = "Tom";
		storedEntry.lname = "Sim";

		var properEntry = {};
		properEntry.id = "a";
		properEntry.fname = "Tom";
		properEntry.lname = "Sim";

		utilities.produceUnmarshallerDataFunction(schemaWithoutNumbers, undefined, function(unmarshaller) {

			unmarshaller([ storedEntry ], function(entry) {
				test.deepEqual([ properEntry ], entry, "There should be no change in the entry");
				test.done();
			});

		});
	},

	shouldBeAbleToUnmarshallDataWithoutAggreg : function(test) {
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

		utilities.produceUnmarshallerDataFunction(schemaWithNumbers, undefined, function(unmarshaller) {

			unmarshaller([ storedEntry ], function(entry) {
				test.deepEqual([ properEntry ], entry, "There should be change in the entry");
				test.done();
			});

		});

	},

	shouldBeAbleToUnmarshallAllData : function(test) {
		var storedEntry = {};
		storedEntry.id = "a";
		storedEntry.fname = "Tom";
		storedEntry.lname = "Sim";
		storedEntry.age = "18";
		storedEntry.salary = "2.2";
		storedEntry['COUNT(id)'] = "2";
		storedEntry['COUNT(fname)'] = "32";

		var properEntry = {};
		properEntry.id = "a";
		properEntry.fname = "Tom";
		properEntry.lname = "Sim";
		properEntry.age = 18;
		properEntry.salary = 2.2;

		properEntry['COUNT(id)'] = 2;
		properEntry['COUNT(fname)'] = 32;

		var aggregations = [ {
			'typ' : 'COUNT',
			'prop' : 'id'
		}, {
			'typ' : 'COUNT',
			'prop' : 'fname'
		} ];
		// TODO
		utilities.produceUnmarshallerDataFunction(schemaWithNumbers, aggregations, function(unmarshaller) {

			unmarshaller([ storedEntry ], function(entry) {
				test.deepEqual([ properEntry ], entry, "There should be change in the entry");
				test.done();
			});

		});
	}

};