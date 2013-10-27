/**
 * New node file
 */

function IndexSearch(phtInterface) {
	var self = this instanceof IndexSearch ? this : Object.create(IndexSearch.prototype);
	self.phtInterface = phtInterface;
	return self;
}

IndexSearch.prototype.searchIndexTable = function(whereClause, indexes, callback) {
	async.parallel([ lookIndexes(whereClause, 0, indexes) ], function(err, results) {
		callback(results);
	});
};

module.exports.IndexSearch = IndexSearch;

var async = require('async');
var curry = require('curry');

var intersectionFunction = function(left, right) {
	var data = [];
	left.forEach(function(elem, s, ss) {
		if (right.indexOf(elem) > -1) {
			data.push(elem);
		}
	});
	return data;
};

var unionFunction = function(left, right) {
	var data = left;
	right.forEach(function(elem, s, ss) {
		if (left.indexOf(elem) == -1) {
			data.push(elem);
		}
	});
	return data;
};

var combiningFunction = curry(function(combineFunction, tag, childs, callback) {
	async.parallel(childs, function(err, results) {
		var data = [];
		data[tag] = [];
		if (err) {
			// TODO DO something on error
		} else {
			var childsLengths = results.length;
			var dataL = results.reduce(function(previousValue, currentValue, index, array) {
				if (currentValue[index].indexOf("PASS_NO_IND") > -1) {
					childsLengths = childsLengths - 1;
					return previousValue;
				} else {
					if (index === 0) {
						return currentValue[0];
					}
					return combineFunction(previousValue, currentValue[index]);
				}
			}, []);
			if (childsLengths === 0) {
				data[tag] = "PASS_NO_IND";
			} else {
				data[tag] = dataL;
			}
		}
		callback(null, data);
	});
});

var search = curry(function(tag, namespace, key, value, operation, callback) {
	// TODO contact PHT for result
	var data = [];
	data[tag] = [ namespace, "test", "test2" ];
	callback(null, data);
});

var noOperation = curry(function(tag, callback) {
	var data = [];
	data[tag] = "PASS_NO_IND";
	callback(null, data);
});

var lookIndexes = function(whereClause, tag, indexes) {
	if (whereClause.logic !== undefined) {

		var operations = whereClause.terms.map(function(elem, ind, smth) {
			return lookIndexes(elem, ind, indexes);
		});
		if (whereClause.logic == 'and') {
			return combiningFunction(intersectionFunction, tag, operations);
		} else {
			return combiningFunction(unionFunction, tag, operations);
		}
	} else {
		if (whereClause.operation === undefined) {
			return noOperation(tag);
		}
		//TODO fix
		return search(tag, "Customers", whereClause.left, whereClause.right, whereClause.operation);
	}
};
