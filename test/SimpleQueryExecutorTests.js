/**
 * New node file
 */

var mockStorageApis = new require('./MockStorageAPIS').MockStorageAPIS();
var builderBuilder = new require('../lib/QueryConfigurationBuilder');

var data = [ {
	'id' : 1,
	'num' : 3
}, {
	'id' : 2,
	'num' : 4
}, {
	'id' : 3,
	'num' : 3
}, {
	'id' : 4,
	'num' : 2
} ];

var mockLScan = function(namespace) {
	return data;
};

var props = [ '*' ];

var namespace = 'noneed';

mockStorageApis.setLscan(mockLScan);
var queryExecutor = new require('../lib/QueryExecutor').QueryExecutor(mockStorageApis);

exports.simpleTests = {
	shouldNotReturnDataOnNoDataPassingFilter : function(test) {
		var builder = builderBuilder.QueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations([]);
		
		var filterPlan = {
			'operator' : '=',
			'left' : 'id',
			'right' : 5
		};

		queryExecutor.formatSelectProperties(props, function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
			queryExecutor.registerNewQuery(builder.buildQueryConfig(), function(err, data) {
				test.equal(0, data.length, 'Should not match data.');
				test.done();
			}, function(err, id) {
				queryExecutor.updateWithLocalMatch(id);
			});
		});

	},
	shouldreturnDataOnDataPassingFilter : function(test) {
		var builder = builderBuilder.QueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations([]);
		
		var filterPlan = {
			'operator' : '=',
			'left' : 'id',
			'right' : 2
		};

		queryExecutor.formatSelectProperties(props, function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
			queryExecutor.registerNewQuery(builder.buildQueryConfig(), function(err, data) {
				test.equal(1, data.length, 'Should not match data.');
				test.done();
			}, function(err, id) {
				queryExecutor.updateWithLocalMatch(id);
			});
		});
	},

	shouldReturnDataOnObjectCountStopper : function(test) {
		var builder = builderBuilder.QueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations([]);
		
		var filterPlan = {
			'operator' : '=',
			'left' : 'num',
			'right' : 3
		};
		queryExecutor.formatSelectProperties(props, function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setMaxObjects(
					2);
			queryExecutor.registerNewQuery(builder.buildQueryConfig(), function(err, data) {
				test.equal(2, data.length, 'Should return two.');
				test.notEqual(2, data[0]['id'], 'Should not have id 2');
				test.notEqual(2, data[1]['id'], 'Should not have id 2');
				test.done();
			}, function(err, id) {
				queryExecutor.updateWithLocalMatch(id);
			});
		});
	},
	shouldReturnDataWithAggregation : function(test) {
		var builder = builderBuilder.QueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations([]);
		
		var filterPlan = {
			'operator' : '>',
			'left' : 'id',
			'right' : 1
		};
		queryExecutor.formatSelectProperties([ 'SUM(num)' ], function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
			queryExecutor.registerNewQuery(builder.buildQueryConfig(), function(err, data) {
				test.equal(1, Object.keys(data).length, 'Should return one object.');
				test.equal(9, data[0]['SUM(num)'], 'Should sum to 9');
				test.done();
			}, function(err, id) {
				queryExecutor.updateWithLocalMatch(id);
			});
		});
	},
	shouldReturnDataWithAggregationAndOtherValues : function(test) {
		var builder = builderBuilder.QueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations([]);
		
		var filterPlan = {
			'operator' : '>',
			'left' : 'id',
			'right' : 1
		};

		queryExecutor.formatSelectProperties([ 'id', 'SUM(num)' ], function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
			queryExecutor.registerNewQuery(builder.buildQueryConfig(), function(err, data) {
				test.equal(3, Object.keys(data).length, 'Should return 3 objects.');
				test.equal(9, data[0]['SUM(num)'], 'Should sum to 9');
				test.equal(2, data[0]['id'], 'Should sum to 9');
				test.done();
			}, function(err, id) {
				queryExecutor.updateWithLocalMatch(id);
			});
		});
	},
	shouldAggregationAndPropSameAsTheAggregation : function(test) {
		var builder = builderBuilder.QueryConfigurationBuilder();
		builder = builder.setNamespace(namespace).setDestinations([]);
		
		var filterPlan = {};
		queryExecutor.formatSelectProperties([ 'num', 'AVR(num)' ], function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
			queryExecutor.registerNewQuery(builder.buildQueryConfig(), function(err, data) {
				test.equal(4, Object.keys(data).length, 'Should return 4 objects.');
				test.equal(3, data[0]['AVR(num)'], 'Should avr to 3');
				test.equal(4, data[1]['num'], 'Should equal to 4');
				test.done();
			}, function(err, id) {
				queryExecutor.updateWithLocalMatch(id);
			});
		});
	}
};
