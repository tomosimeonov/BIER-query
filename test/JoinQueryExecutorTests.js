/**
 * New node file
 */

var mockStorageApis = new require('./MockStorageAPIS').MockStorageAPIS();
var builderBuilder = new require('../lib/builders/JoinQueryConfigurationBuilder');
var messageBuilderBuilder = require('../lib/builders/QueryMessageBuilder');
var statistics = new require('../lib/StatisticHolder').StatisticHolder();
var data_namespaceOne = {
	'01' : {
		'id' : 1,
		'num' : 3,
		'one' : 1
	},
	'02' : {
		'id' : 2,
		'num' : 4,
		'one' : 1
	},
	'03' : {
		'id' : 3,
		'num' : 3,
		'one' : 1
	},
	'04' : {
		'id' : 4,
		'num' : 2,
		'one' : 1
	}
};

var data_namespaceTwo = {
	'01' : {
		'id' : 1,
		'num' : 3,
		'two' : 2
	},
	'02' : {
		'id' : 2,
		'num' : 4,
		'two' : 2
	},
	'03' : {
		'id' : 3,
		'num' : 3,
		'two' : 2
	},
	'04' : {
		'id' : 4,
		'num' : 2,
		'two' : 2
	}
};

var data_namespaceThree = {
	'01' : {
		'id' : 1,
		'num' : 3,
		'three' : 3
	},
	'02' : {
		'id' : 2,
		'num' : 4,
		'three' : 3
	},
	'03' : {
		'id' : 3,
		'num' : 3,
		'three' : 3
	},
	'04' : {
		'id' : 4,
		'num' : 2,
		'three' : 3
	}
};

var mockLScan = function(namespace, callback) {
	var passed = false;
	switch (namespace) {
	case 'one':
		passed = true;
		callback(data_namespaceOne);
		break;
	case 'two':
		passed = true;
		callback(data_namespaceTwo);
		break;
	case 'three':
		passed = true;
		callback(data_namespaceThree);
		break;
	}
	if (!passed)
		callback({});
};

var props = [ '*' ];

var namespaceOne = 'one';
var namespaceTwo = 'two';
mockStorageApis.setLscan(mockLScan);

exports.simpleTests = {
	shouldNotReturnDataOnNoDataPassingFilter : function(test) {
		var queryExecutor = new require('../lib/executors/JoinExecutor').JoinExecutor(mockStorageApis, statistics);

		var builder = builderBuilder.JoinQueryConfigurationBuilder();
		builder = builder.setNamespaceOne(namespaceOne).setNamespaceTwo(namespaceTwo).setJoinPropertyOne('num')
				.setJoinPropertyTwo('num').setFullType(0);

		var filterPlan = {
			'operator' : '=',
			'left' : 'one.id',
			'right' : 5
		};

		queryExecutor.formatSelectProperties(props, function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(4);
			queryExecutor.registerNewQuery(builder.buildQueryConfig(), function(err, data) {
				test.equal(0, data.length, 'Should not match data.');
				test.done();
			}, function(err, id) {
				var localDataV = Object.keys(data_namespaceTwo).map(function(key) {
					return data_namespaceTwo[key];
				});

				var message = messageBuilderBuilder.QueryMessageBuilder().setQueryId(id).setJoinType()
						.setOrigin("test").setPayload(localDataV).setCustomPayloadType("SEC_DATA").buildMessage();

				var broadcast = function(a, b, c) {
					var mess = JSON.parse(a)
					if (mess.payload !== undefined && mess.payload.datasearch !== undefined)
						setTimeout(function() {
							queryExecutor.message(message);
						}, 500);
				};
				mockStorageApis.setBroadcast(broadcast);

				queryExecutor.executeQuery(id);
			});
		});

	},
	shouldreturnDataOnDataPassingFilter : function(test) {
		var queryExecutor = new require('../lib/executors/JoinExecutor').JoinExecutor(mockStorageApis, statistics);

		var builder = builderBuilder.JoinQueryConfigurationBuilder();
		builder = builder.setNamespaceOne(namespaceOne).setNamespaceTwo(namespaceTwo).setJoinPropertyOne('id')
				.setJoinPropertyTwo('id').setFullType(0);

		var filterPlan = {
			'operator' : '=',
			'left' : 'one.id',
			'right' : 2
		};

		queryExecutor.formatSelectProperties(props, function(err, formatedSelectProperties) {
			builder = builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(4);
			queryExecutor.registerNewQuery(builder.buildQueryConfig(), function(err, data) {
				test.equal(1, data.length, 'Should match data.');
				test.done();
			}, function(err, id) {

				var localDataV = Object.keys(data_namespaceTwo).map(function(key) {
					return data_namespaceTwo[key];
				});
				var message = messageBuilderBuilder.QueryMessageBuilder().setQueryId(id).setJoinType()
						.setOrigin("test").setPayload(localDataV).setCustomPayloadType("SEC_DATA").buildMessage();

				var broadcast = function(a, b, c) {
					var mess = JSON.parse(a)
					if (mess.payload !== undefined && mess.payload.datasearch !== undefined)
					setTimeout(function() {
						queryExecutor.message(message);
					}, 500);
				};
				mockStorageApis.setBroadcast(broadcast);
				queryExecutor.executeQuery(id);
			});
		});
	}
// ,
//
// shouldReturnDataOnObjectCountStopper : function(test) {
// var builder = builderBuilder.SimpleQueryConfigurationBuilder();
// builder = builder.setNamespace(namespace).setDestinations([]);
//
// var filterPlan = {
// 'operator' : '=',
// 'left' : 'num',
// 'right' : 3
// };
// queryExecutor.formatSelectProperties(props, function(err,
// formatedSelectProperties) {
// builder =
// builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setMaxObjects(
// 2);
// queryExecutor.registerNewQuery(builder.buildQueryConfig(), function(err,
// data) {
// test.equal(2, data.length, 'Should return two.');
// test.notEqual(2, data[0]['id'], 'Should not have id 2');
// test.notEqual(2, data[1]['id'], 'Should not have id 2');
// test.done();
// }, function(err, id) {
// queryExecutor.executeQuery(id);
// });
// });
// },
// shouldReturnDataWithAggregation : function(test) {
// var builder = builderBuilder.SimpleQueryConfigurationBuilder();
// builder = builder.setNamespace(namespace).setDestinations([]);
//
// var filterPlan = {
// 'operator' : '>',
// 'left' : 'id',
// 'right' : 1
// };
// queryExecutor.formatSelectProperties([ 'SUM(num)' ], function(err,
// formatedSelectProperties) {
// builder =
// builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
// queryExecutor.registerNewQuery(builder.buildQueryConfig(), function(err,
// data) {
// test.equal(1, Object.keys(data).length, 'Should return one object.');
// test.equal(9, data[0]['SUM(num)'], 'Should sum to 9');
// test.done();
// }, function(err, id) {
// queryExecutor.executeQuery(id);
// });
// });
// },
// shouldReturnDataWithAggregationAndOtherValues : function(test) {
// var builder = builderBuilder.SimpleQueryConfigurationBuilder();
// builder = builder.setNamespace(namespace).setDestinations([]);
//
// var filterPlan = {
// 'operator' : '>',
// 'left' : 'id',
// 'right' : 1
// };
//
// queryExecutor.formatSelectProperties([ 'id', 'SUM(num)' ], function(err,
// formatedSelectProperties) {
// builder =
// builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
// queryExecutor.registerNewQuery(builder.buildQueryConfig(), function(err,
// data) {
// test.equal(3, Object.keys(data).length, 'Should return 3 objects.');
// test.equal(9, data[0]['SUM(num)'], 'Should sum to 9');
// test.equal(2, data[0]['id'], 'Should sum to 9');
// test.done();
// }, function(err, id) {
// queryExecutor.executeQuery(id);
// });
// });
// },
// shouldAggregationAndPropSameAsTheAggregation : function(test) {
// var builder = builderBuilder.SimpleQueryConfigurationBuilder();
// builder = builder.setNamespace(namespace).setDestinations([]);
//
// var filterPlan = {};
// queryExecutor.formatSelectProperties([ 'num', 'AVR(num)' ], function(err,
// formatedSelectProperties) {
// builder =
// builder.setFilterPlan(filterPlan).setFormattedProperties(formatedSelectProperties).setTimeout(1);
// queryExecutor.registerNewQuery(builder.buildQueryConfig(), function(err,
// data) {
// test.equal(4, Object.keys(data).length, 'Should return 4 objects.');
// test.equal(3, data[0]['AVR(num)'], 'Should avr to 3');
// test.equal(4, data[1]['num'], 'Should equal to 4');
// test.done();
// }, function(err, id) {
// queryExecutor.executeQuery(id);
// });
// });
// }
};
