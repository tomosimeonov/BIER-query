/**
 * New node file
 */

var mockStorageApis = new require('./MockStorageAPIS').MockStorageAPIS();
var builderBuilder = new require('../lib/builders/SimpleQueryConfigurationBuilder');
var statistics = new require('../lib/StatisticHolder').StatisticHolder();

var emiterBuilder = require('events');

var data = {
	'01' : {
		'id' : 1,
		'num' : 3
	},
	'02' : {
		'id' : 2,
		'num' : 4
	},
	'03' : {
		'id' : 3,
		'num' : 3
	},
	'04' : {
		'id' : 4,
		'num' : 2
	}
};

var mockLScan = function(namespace, callback) {
	callback(data);
};

var props = [ '*' ];

var namespace = 'noneed';

mockStorageApis.setLscan(mockLScan);
var queryExecutor = new require('../lib/executors/SimpleExecutor').SimpleExecutor(mockStorageApis, statistics);

var buildEmitter = function(checker) {
	var dataHolder = [];
	var emiter = new emiterBuilder.EventEmitter();
	emiter = emiter.on('data', function(data) {
		dataHolder = dataHolder.concat(data);
	}).on('end', function() {
		checker(dataHolder);
	});
	return emiter;
};

exports.simpleTests = {
	shouldNotReturnDataOnNoDataPassingFilter : function(test) {
		var builder = builderBuilder.SimpleQueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations([]);

		var emiter = buildEmitter(function(dataHolder) {
			test.equal(0, dataHolder.length, 'Should not match data.');
			test.done();
		});

		var filterPlan = {
			'operator' : '=',
			'left' : 'id',
			'right' : 5
		};

		queryExecutor.formatSelectProperties(props, function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
			queryExecutor.executeQuery(builder.buildQueryConfig(), emiter);
		});

	},
	shouldreturnDataOnDataPassingFilter : function(test) {
		var builder = builderBuilder.SimpleQueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations([]);

		var emiter = buildEmitter(function(data) {
			test.equal(1, data.length, 'Should not match data.');
			test.done();
		});

		var filterPlan = {
			'operator' : '=',
			'left' : 'id',
			'right' : 2
		};

		queryExecutor.formatSelectProperties(props, function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
			queryExecutor.executeQuery(builder.buildQueryConfig(), emiter);
		});
	},

	shouldReturnDataOnObjectCountStopper : function(test) {
		var builder = builderBuilder.SimpleQueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations([]);

		var emiter = buildEmitter(function(data) {
			test.equal(2, data.length, 'Should return two.');
			test.notEqual(2, data[0]['id'], 'Should not have id 2');
			test.notEqual(2, data[1]['id'], 'Should not have id 2');
			test.done();
		});

		var filterPlan = {
			'operator' : '=',
			'left' : 'num',
			'right' : 3
		};
		queryExecutor.formatSelectProperties(props, function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setMaxObjects(
					2);
			queryExecutor.executeQuery(builder.buildQueryConfig(), emiter);
		});
	},
	shouldReturnDataWithAggregation : function(test) {
		var builder = builderBuilder.SimpleQueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations([]);

		var emiter = buildEmitter(function(data) {
			test.equal(1, Object.keys(data).length, 'Should return one object.');
			test.equal(9, data[0]['SUM(num)'], 'Should sum to 9');
			test.done();
		});

		var filterPlan = {
			'operator' : '>',
			'left' : 'id',
			'right' : 1
		};
		queryExecutor.formatSelectProperties([ 'SUM(num)' ], function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
			queryExecutor.executeQuery(builder.buildQueryConfig(), emiter);
		});
	},
	shouldReturnDataWithAggregationAndOtherValues : function(test) {
		var builder = builderBuilder.SimpleQueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations([]);

		var emiter = buildEmitter(function(data) {
			test.equal(3, Object.keys(data).length, 'Should return 3 objects.');
			test.equal(9, data[0]['SUM(num)'], 'Should sum to 9');
			test.equal(2, data[0]['id'], 'Should sum to 9');
			test.done();
		});

		var filterPlan = {
			'operator' : '>',
			'left' : 'id',
			'right' : 1
		};

		queryExecutor.formatSelectProperties([ 'id', 'SUM(num)' ], function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
			queryExecutor.executeQuery(builder.buildQueryConfig(), emiter);
		});
	},
	shouldAggregationAndPropSameAsTheAggregation : function(test) {
		var builder = builderBuilder.SimpleQueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations([]);

		var emiter = buildEmitter(function(data) {
			test.equal(4, Object.keys(data).length, 'Should return 4 objects.');
			test.equal(3, data[0]['AVR(num)'], 'Should avr to 3');
			test.equal(4, data[1]['num'], 'Should equal to 4');
			test.done();
		});

		var filterPlan = {};
		queryExecutor.formatSelectProperties([ 'num', 'AVR(num)' ], function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
			queryExecutor.executeQuery(builder.buildQueryConfig(), emiter);
		});
	}
};
