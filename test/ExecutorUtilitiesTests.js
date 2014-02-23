/**
 * New node file
 */
var utilities = require('../lib/executors/common/Utilities');
var extend = require('util')._extend;
var messageBuilderBuilder = require('../lib/builders/QueryMessageBuilder');

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
				test.equal(true, entry[0].age > 9);
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

		var tester = {};
		tester["a"] = storedEntry;
		utilities.produceUnmarshallerDataFunction(schemaWithNumbers, aggregations, function(unmarshaller) {

			unmarshaller(tester, function(entry) {
				tester["a"] = properEntry;

				test.deepEqual(tester, entry, "There should be change in the entry");
				test.equal("number", typeof entry["a"].age, "Should be of type number");
				test.done();
			});

		});
	}

};

exports.messageFragmentation = {
	shouldNotFragmentSmallMessage : function(test) {
		var i = 0;
		var store = {};
		store.Node = {};
		store.Node.send = function(data) {
			fired = true;
			i = i + 1;
			test.equal(i, 1, "Should be called only once");
		};

		var payload = {};
		payload.a = {};
		payload.a.a = 1;
		payload.a.b = 2;
		payload.b = {};
		payload.b.a = 3;
		payload.b.b = 4;

		var messageB = messageBuilderBuilder.QueryMessageBuilder().setQueryId("test").setSimpleType().setOrigin("test")
				.setPayloadTypeData();

		utilities.sendDataMessage("test", messageB, payload, store);
		setTimeout(function() {
			test.equal(fired, true, "Should be called"), test.done();
		}, 1000);
	},

	shouldFragmentBigMessage : function(test) {
		var i = 0;
		var store = {};
		store.Node = {};
		store.Node.send = function(data) {
			fired = true;
			i = i + 1;
			test.equal(i <= 2, true, "Should be called more than once");
		};

		var fired = false;
		var payload = {};
		payload.a = {};
		payload.b = {};
		payload.a.a = "";
		payload.a.b = "";
		payload.b.a = "";
		payload.b.b = "";
		for (var ii = 0; ii < 100000; ii++) {
			payload.a.a = payload.a.a + "123456789101112131415161718192021222324252627282930";
			payload.a.b = payload.a.b + "123456789101112131415161718192021222324252627282930";
			payload.b.a = payload.b.a + "123456789101112131415161718192021222324252627282930";
			payload.b.b = payload.b.b + "123456789101112131415161718192021222324252627282930";
		}

		var messageB = messageBuilderBuilder.QueryMessageBuilder().setQueryId("test").setSimpleType().setOrigin("test")
				.setPayloadTypeData();

		utilities.sendDataMessage("test", messageB, payload, store);
		setTimeout(function() {
			test.equal(i, 2, "Not correct");
			test.equal(fired, true, "Should be called"), test.done();
		}, 4000);
	}
};