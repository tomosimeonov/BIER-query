/**
 * New node file
 */
function smaller(propertyTable,value,callback){
	callback(null,[propertyTable, value]);
}

function smallerEq(propertyTable,value,callback){
	callback(null,[propertyTable, value]);
}

function greater(propertyTable,value,callback){
	callback(null,[propertyTable, value]);
}

function greaterEq(propertyTable,value,callback){
	callback(null,[propertyTable, value]);
}

module.exports.smaller = smaller;
module.exports.smallerEq = smallerEq;
module.exports.greater = greater;
module.exports.greaterEq = greaterEq;