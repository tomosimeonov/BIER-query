/**
 * Testing filter builder functionality
 */

var filterBuilder = require('../lib/FilterBuilder'); // .createFilterFromWhereClause

//Data 
var simpleWhereClauses = [ {
	'operator' : '=',
	'left' : 'id',
	'right' : 2
}, {
	'operator' : '>',
	'left' : 'id',
	'right' : 2
}, {
	'operator' : '>=',
	'left' : 'id',
	'right' : 2
}, {
	'operator' : '<',
	'left' : 'id',
	'right' : 2
}, {
	'operator' : '<=',
	'left' : 'id',
	'right' : 2
}, {
	'operator' : '!=',
	'left' : 'id',
	'right' : 3
}, {
	'operator' : '<>',
	'left' : 'id',
	'right' : 3
} ];

var complexWhereClauses = [ {
	'logic' : 'and',
	'terms' : [ {
		'operator' : '=',
		'left' : 'id',
		'right' : 2
	}, {
		'operator' : '=',
		'left' : 'name',
		'right' : 'A'
	} ]
}, {
	'logic' : 'or',
	'terms' : [ {
		'operator' : '=',
		'left' : 'id',
		'right' : 2
	}, {
		'operator' : '=',
		'left' : 'name',
		'right' : 'A'
	} ]
}, {
	'logic' : 'or',
	'terms' : [ {
		'logic' : 'and',
		'terms' : [ {
			'operator' : '=',
			'left' : 'id',
			'right' : 2
		}, {
			'operator' : '=',
			'left' : 'name',
			'right' : 'A'
		} ]
	}, {
		'logic' : 'or',
		'terms' : [ {
			'operator' : '=',
			'left' : 'id',
			'right' : 2
		}, {
			'operator' : '=',
			'left' : 'name',
			'right' : 'A'
		} ]
	} ]
},
{
	'logic' : 'and',
	'terms' : [ {
		'logic' : 'and',
		'terms' : [ {
			'operator' : '=',
			'left' : 'id',
			'right' : 2
		}, {
			'operator' : '=',
			'left' : 'name',
			'right' : 'A'
		} ]
	}, {
		'logic' : 'or',
		'terms' : [ {
			'operator' : '=',
			'left' : 'id',
			'right' : 2
		}, {
			'operator' : '=',
			'left' : 'name',
			'right' : 'A'
		} ]
	} ]
}];

var datas = [ {
	'id' : 2
}, {
	'id' : 3
}, {
	'id' : 1
} ];

var complexDatas = [ {
	'id' : 2,
	'name' : 'B'
}, {
	'id' : 2,
	'name' : 'A'
}, {
	'id' : 3,
	'name' : 'B'
} ];

//Tests
exports.simpleBuilder = {
	shouldProduceFilterFromOnlyEq : function(test) {
		filterBuilder.createFilterFromWhereClause(simpleWhereClauses[0], function(filter) {
			test.equal(true, filter(datas[0]), "Should pass the first data");
			test.equal(false, filter(datas[1]), "Should not pass the second data");
			test.done();
		});
	},
	shouldProduceFilterFromOlnyLess : function(test) {
		filterBuilder.createFilterFromWhereClause(simpleWhereClauses[1], function(filter) {
			test.equal(false, filter(datas[0]), "Should not pass the first data");
			test.equal(true, filter(datas[1]), "Should pass the second data");
			test.equal(false, filter(datas[2]), "Should not pass the third data");
			test.done();
		});
	},
	shouldProduceFilterFromOnlyLessEq : function(test) {
		filterBuilder.createFilterFromWhereClause(simpleWhereClauses[2], function(filter) {
			test.equal(true, filter(datas[0]), "Should pass the first data");
			test.equal(true, filter(datas[1]), "Should pass the second data");
			test.equal(false, filter(datas[2]), "Should not pass the third data");
			test.done();
		});
	},
	shouldProduceFilterFromOlnyGreater : function(test) {
		filterBuilder.createFilterFromWhereClause(simpleWhereClauses[3], function(filter) {
			test.equal(false, filter(datas[0]), "Should not pass the first data");
			test.equal(false, filter(datas[1]), "Should not pass the second data");
			test.equal(true, filter(datas[2]), "Should pass the third data");
			test.done();
		});
	},
	shouldProduceFilterFromOnlyGreaterEq : function(test) {
		filterBuilder.createFilterFromWhereClause(simpleWhereClauses[4], function(filter) {
			test.equal(true, filter(datas[0]), "Should pass the first data");
			test.equal(false, filter(datas[1]), "Should not pass the second data");
			test.equal(true, filter(datas[2]), "Should pass the third data");
			test.done();
		});
	},
	shouldProduceFilterFromOnlyNotEqOne : function(test) {
		filterBuilder.createFilterFromWhereClause(simpleWhereClauses[5], function(filter) {
			test.equal(true, filter(datas[0]), "Should pass the first data");
			test.equal(false, filter(datas[1]), "Should not pass the second data");
			test.equal(true, filter(datas[2]), "Should pass the third data");
			test.done();
		});
	},
	shouldProduceFilterFromOnlyNotEqTwo : function(test) {
		filterBuilder.createFilterFromWhereClause(simpleWhereClauses[5], function(filter) {
			test.equal(true, filter(datas[0]), "Should pass the first data");
			test.equal(false, filter(datas[1]), "Should not pass the second data");
			test.equal(true, filter(datas[2]), "Should pass the third data");
			test.done();
		});
	}

};

exports.complexBuilder = {
	shouldProduceFilterFromSimpleAnd : function(test) {
		filterBuilder.createFilterFromWhereClause(complexWhereClauses[0], function(filter) {
			test.equal(false, filter(complexDatas[0]), "Should not pass the first data");
			test.equal(true, filter(complexDatas[1]), "Should not pass the second data");
			test.equal(false, filter(complexDatas[2]), "Should not pass the third data");
			test.done();
		});
	},
	shouldProduceFilterFromSimpleOr : function(test) {
		filterBuilder.createFilterFromWhereClause(complexWhereClauses[1], function(filter) {
			test.equal(true, filter(complexDatas[0]), "Should pass the first data");
			test.equal(true, filter(complexDatas[1]), "Should pass the second data");
			test.equal(false, filter(complexDatas[2]), "Should not pass the third data");
			test.done();
		});
	},
	shouldProduceFilterFromComplexOr : function(test) {
		filterBuilder.createFilterFromWhereClause(complexWhereClauses[2], function(filter) {
			test.equal(true, filter(complexDatas[0]), "Should pass the first data");
			test.equal(true, filter(complexDatas[1]), "Should pass the second data");
			test.equal(false, filter(complexDatas[2]), "Should not pass the third data");
			test.done();
		});
	},
	shouldProduceFilterFromComplexAnd : function(test) {
		filterBuilder.createFilterFromWhereClause(complexWhereClauses[3], function(filter) {
			test.equal(false, filter(complexDatas[0]), "Should not pass the first data");
			test.equal(true, filter(complexDatas[1]), "Should pass the second data");
			test.equal(false, filter(complexDatas[2]), "Should not pass the third data");
			test.done();
		});
	}
}