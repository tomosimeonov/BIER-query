/**
 * Methods to extract properties from query plan.
 * 
 * @author Tomo Simeonov
 */

// Helper function
var updateHolder = function(table, property, propertiesHolder) {
	if (propertiesHolder[table] === undefined)
		propertiesHolder[table] = [ property ];
	else if (propertiesHolder[table].indexOf(property) == -1)
		propertiesHolder[table].push(property);
};

// Helper function
var insertPropertyIntoHolder = function(property, propertiesHolder) {
	if (property.indexOf('.') > -1) {
		var tableSpecific = property.split('.');
		updateHolder(tableSpecific[0], tableSpecific[1], propertiesHolder);
	} else if (property != '*')
		updateHolder('default', property, propertiesHolder);
};

// Helper function to check where clause
var whereTreeExplorer = function(whereProperties, propertiesHolder, matcherStrategyFunction,
		exploreTreeStartegyFunction) {
	for ( var property in whereProperties) {
		if (property == "terms")
			whereProperties[property].forEach(exploreTreeStartegyFunction);
		if (matcherStrategyFunction(property))
			insertPropertyIntoHolder(whereProperties[property], propertiesHolder);
	}
};

/**
 * Method to extract Select properties, if any from the plan
 */
var extractSelectProperties = function(plan, callback) {

	var propertiesHolder = [];

	if (plan.SELECT) {
		var forEachWrapperForInsert = function(element, index, array) {
			insertPropertyIntoHolder(element, propertiesHolder);
		};

		plan.SELECT.forEach(forEachWrapperForInsert);
	}

	callback(propertiesHolder);
};

/**
 * Method to extract Join properties, if any from the plan
 */
var extractJoinProperties = function(plan, callback) {

	var propertiesHolder = [];

	var matcherJoinStrategyFunction = function(property) {
		return property == "left" || property == "right";
	};
	var exploreTreeJoinStrategyFunction = function(element, index, array) {
		whereTreeExplorer(element, propertiesHolder, matcherJoinStrategyFunction, exploreTreeJoinStrategyFunction);
	};

	if (plan['INNER JOIN'] !== undefined) {
		whereTreeExplorer(plan['INNER JOIN'].cond, propertiesHolder, matcherJoinStrategyFunction,
				exploreTreeJoinStrategyFunction);
	} else if (plan['LEFT JOIN'] !== undefined) {
		whereTreeExplorer(plan['LEFT JOIN'].cond, propertiesHolder, matcherJoinStrategyFunction,
				exploreTreeJoinStrategyFunction);
	} else if (plan['RIGHT JOIN'] !== undefined) {
		whereTreeExplorer(plan['RIGHT JOIN'].cond, propertiesHolder, matcherJoinStrategyFunction,
				exploreTreeJoinStrategyFunction);
	}
	callback(propertiesHolder);
};

/**
 * Method to extract all properties in the plan
 */
var extractWhereProperties = function(plan, callback) {

	var propertiesHolder = [];

	var matcherNONJoinStrategyFunction = function(property) {
		return property == "left";
	};

	var exploreTreeNoJoinStrategyFunction = function(element, index, array) {
		whereTreeExplorer(element, propertiesHolder, matcherNONJoinStrategyFunction, exploreTreeNoJoinStrategyFunction);
	};

	if (plan.WHERE)
		whereTreeExplorer(plan.WHERE, propertiesHolder, matcherNONJoinStrategyFunction,
				exploreTreeNoJoinStrategyFunction);
	callback(propertiesHolder);
};

/**
 * Extract all properties used in the plan.
 */
var extractAllProperties = function(plan, callback) {

	var propertiesHolder = [];

	var combineWithPropertiesHolder = function(array) {
		for ( var prop in array) {
			if (propertiesHolder[prop] === undefined)
				propertiesHolder[prop] = array[prop];
			else
				propertiesHolder[prop] = propertiesHolder[prop].concat(array[prop]);
		}
	};

	var whereFunction = function(data) {
		combineWithPropertiesHolder(data);
		callback(propertiesHolder);
	};

	var joinFunction = function(data) {
		combineWithPropertiesHolder(data);
		extractWhereProperties(plan, whereFunction);
	};

	var selectFunction = function(data) {
		propertiesHolder = data;
		extractJoinProperties(plan, joinFunction);
	};

	extractSelectProperties(plan, selectFunction);
};

module.exports.extractAllProperties = extractAllProperties;
module.exports.extractSelectProperties = extractSelectProperties;
module.exports.extractJoinProperties = extractJoinProperties;
module.exports.extractWhereProperties = extractWhereProperties;
