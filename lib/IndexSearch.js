/**
 * This object is responsible for querying the PHT for indexes values from
 * properties
 */

function IndexSearch(phtWrapper) {	
	var self = this instanceof IndexSearch ? this : Object.create(IndexSearch.prototype);	
	self.phtWrapper = phtWrapper;
	return self;
}

IndexSearch.prototype.searchIndexTable = function(namespace, whereClause, indexes, callback) {
	
	var phtWrapper = this.phtWrapper;
	// Internal methods
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
				callback(err, null);
			} else {
				var childsLengthsWithourNoInd = results.length;
				
				// combines data
				var dataL = results.reduce(function(previousValue, currentValue, index, array) {
					if (currentValue[index].indexOf(phtWrapper.pastNoIndTag) > -1) {
						childsLengthsWithourNoInd = childsLengthsWithourNoInd - 1;
						return previousValue;
					} else {
						if (index === 0) {
							
							return currentValue[0];
						}
						return combineFunction(previousValue, currentValue[index]);
					}
				}, []);
				
				//
				if (childsLengthsWithourNoInd === 0) {
					data[tag] = [phtWrapper.pastNoIndTag];
				} else {
					data[tag] = [].concat.apply([], dataL);
				}
			}
			callback(null, data);
		});
	});

	var search = curry(function(tag, namespace, key, value, operation, callback) {
		phtWrapper.executeSearch(namespace, key, value, operation, function(err, ids) {
			if (err) {
				callback(err, null);
			} else {
				var data = [];
				data[tag] = ids;
				callback(null, data);
			}
		});
	});

	var noOperation = curry(function(tag, callback) {
		var data = [];
		data[tag] = [phtWrapper.pastNoIndTag];
		callback(null, data);
	});

	var lookIndexes = function(namespace, whereClause, tag, indexes) {
		if (whereClause.logic) {
			var operations = whereClause.terms.map(function(elem, ind, smth) {
				return lookIndexes(namespace, elem, ind, indexes);
			});
			if (whereClause.logic == 'and') {
				return combiningFunction(intersectionFunction, tag, operations);
			} else {
				return combiningFunction(unionFunction, tag, operations);
			}
		} else if(whereClause.operator)  {
			var property = whereClause.left;
			if(whereClause.left.indexOf(".") > -1){
				property = whereClause.left.split(".")[1];
				if(whereClause.left.split(".")[0] != namespace)
					return noOperation(tag);
			}
			if (indexes.indexOf(property) == -1) {
				return noOperation(tag);
			} else {
				return search(tag, namespace, property, whereClause.right, whereClause.operator);
			}
		} else {
			return [];
		}
	};
	
	async.parallel([ lookIndexes(namespace, whereClause, 0, indexes) ], function(err, results) {
		var merged = [].concat.apply([], results);
		if(merged[0].indexOf(phtWrapper.pastNoIndTag) > -1){
			callback(err, {'ids':[],'sendToAll': true});
		}else{
			callback(err, {'ids':merged[0], 'sendToAll':false});
		}
		
		
	});
};
// EXPORT
module.exports = IndexSearch;