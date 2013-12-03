/**
 * New node file
 */

var mockStorageApis = new require('./MockStorageAPIS').MockStorageAPIS();

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

var filterPlan = {
	'operator' : '=',
	'left' : 'id',
	'right' : 2
};

var props = [ '*' ];

var namespace = 'noneed';

mockStorageApis.setLscan(mockLScan);
var queryExecutor = new require('../lib/QueryExecutor').QueryExecutor(mockStorageApis);

exports.simpleTests = {
	shouldNotReturnDataOnNoDataPassingFilter : function(test) {
		var filterPlan = {
			'operator' : '=',
			'left' : 'id',
			'right' : 5
		};

		queryExecutor.registerNewQuery(undefined, 1, function(err, data) {
			test.equal(0, data.length, 'Should not match data.');
			test.done();
		}, function(err, id) {
			queryExecutor.updateWithLocalMatch(id, props, namespace, filterPlan);
		});
	},
	shouldreturnDataOnDataPassingFilter : function(test) {
		var filterPlan = {
			'operator' : '=',
			'left' : 'id',
			'right' : 2
		};

		queryExecutor.registerNewQuery(undefined, 1, function(err, data) {
			test.equal(1, data.length, 'Should return two.');
			test.equal(4, data[0]['num'], 'Should return num equal to 4');
			test.done();
		}, function(err, id) {
			queryExecutor.updateWithLocalMatch(id, props, namespace, filterPlan);
		});
	},

	shouldReturnDataOnObjectCountStopper : function(test) {
		var filterPlan = {
			'operator' : '=',
			'left' : 'num',
			'right' : 3
		};
		queryExecutor.registerNewQuery(5, undefined, function(err, data) {
			test.equal(2, data.length, 'Should return two.');
			test.notEqual(2, data[0]['id'], 'Should not have id 2');
			test.notEqual(2, data[1]['id'], 'Should not have id 2');
			test.done();
		}, function(err, id) {
			queryExecutor.updateWithLocalMatch(id, props, namespace, filterPlan);
		});
	},
	shouldReturnDataWithAggregation : function(test) {
		var filterPlan = {
			'operator' : '>',
			'left' : 'id',
			'right' : 1
		};
		queryExecutor.registerNewQuery(undefined, 1, function(err, data) {
			test.equal(1, Object.keys(data).length, 'Should return one object.');
			test.equal(9, data[0]['SUM(num)'], 'Should sum to 9');
			test.done();
		}, function(err, id) {
			queryExecutor.updateWithLocalMatch(id, ['SUM(num)' ], namespace, filterPlan);
		});
	},
	shouldReturnDataWithAggregationAndOtherValues : function(test) {
		var filterPlan = {
			'operator' : '>',
			'left' : 'id',
			'right' : 1
		};
		queryExecutor.registerNewQuery(undefined, 1, function(err, data) {
			test.equal(3, Object.keys(data).length, 'Should return one object.');
			test.equal(9, data[0]['SUM(num)'], 'Should sum to 9');
			test.equal(2, data[0]['id'], 'Should sum to 9');
			test.done();
		}, function(err, id) {
			queryExecutor.updateWithLocalMatch(id, ['id','SUM(num)' ], namespace, filterPlan);
		});
	}
};