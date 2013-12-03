/**
 * This function creates filter function from the where clause of the provided plan for local data uses.
 * If the query is a join query the two possible objects need to be combined in one and then run through the filter.
 */

var curry = require('curry');

var eq = curry(function(prop, value, data) {
	return data[prop] === value;
});

var lessEq = curry(function(prop, value, data) {
	return data[prop] <= value;
});

var less = curry(function(prop, value, data) {
	return data[prop] < value;
});

var greater = curry(function(prop, value, data) {
	return data[prop] > value;
});
var greaterEq = curry(function(prop, value, data) {
	return data[prop] >= value;
});
var notEq = curry(function(prop, value, data) {
	return data[prop] != value;
});

var and = curry(function(operations, data) {
	var answer = true;
	for ( var op in operations) {
		if (operations[op](data) === false) {
			answer = false;
			break;
		}
	}
	return answer;
});

var or = curry(function(operations, data) {
	var answer = false;
	for ( var op in operations) {
		if (operations[op](data) === true) {
			answer = true;
			break;
		}
	}
	return answer;
}); 

var createFilterFromWhereClauseL = function(whereClause) {
	if (whereClause.logic !== undefined) {
		operations = whereClause.terms.map(function(elem, ind, smth) {
			return createFilterFromWhereClauseL(elem);
		});
		if (whereClause.logic == 'and') {
			return and(operations);
		} else {
			return or(operations);
		}
	} else if(whereClause.operator){
		switch (whereClause.operator) {
		case '=':
			return eq(whereClause.left, whereClause.right);
		case '<':
			return less(whereClause.left, whereClause.right);
		case '<=':
			return lessEq(whereClause.left, whereClause.right);
		case '>':
			return greater(whereClause.left, whereClause.right);
		case '>=':
			return greaterEq(whereClause.left, whereClause.right);
		case '!=':
			return notEq(whereClause.left, whereClause.right);
		case '<>':
			return notEq(whereClause.left, whereClause.right);

		}
	}else {
		return function(data){return true};
	}
};

/**
 * Creates filter from whereClause for local data uses.
 */
var createFilterFromWhereClause = function(whereClause,callback){
	callback(undefined,createFilterFromWhereClauseL(whereClause));
};

module.exports.createFilterFromWhereClause = createFilterFromWhereClause;