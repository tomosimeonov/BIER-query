/**
 * New node file
 */

var checkSchemaComplience = function(propertiesToCheck, tableSchema, callback) {

	var notFoundProp = propertiesToCheck.length;
	var checkElement = function(element, index, array) {
		if (propertiesToCheck.indexOf(element.name) > -1)
			notFoundProp--;
	};

	tableSchema.definitions.forEach(checkElement);
	callback(notFoundProp === 0);
};

var updateHolder = function(table, property, propertiesHolder) {
	if (propertiesHolder[table] === undefined)
		propertiesHolder[table] = [ property ];
	else if (propertiesHolder[table].indexOf(property) == -1)
		propertiesHolder[table].push(property);
};

var insertPropertyIntoHolder = function(property, propertiesHolder) {
	if (property.indexOf('.') > -1) {
		var tableSpecific = property.split('.');
		updateHolder(tableSpecific[0], tableSpecific[1], propertiesHolder);
	} else if (property != '*')
		updateHolder('default', property, propertiesHolder);
};

var whereTreeExplorer = function(whereProperties, propertiesHolder, matcherStrategyFunction,
		exploreTreeStartegyFunction) {
	for ( var property in whereProperties) {
		if (property == "terms")
			whereProperties[property].forEach(exploreTreeStartegyFunction);
		if (matcherStrategyFunction(property))
			insertPropertyIntoHolder(whereProperties[property], propertiesHolder);
	}
};

var selectPropertyFromQueryPlanExtractor = function(plan, callback) {

	var propertiesHolder = [];

	var forEachWrapperForInsert = function(element, index, array) {
		insertPropertyIntoHolder(element, propertiesHolder);
	};

	plan.SELECT.forEach(forEachWrapperForInsert);
	callback(propertiesHolder);
};

var joinPropertyFromQueryPlanExtractor = function(plan, callback) {

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

var wherePropertyFromQueryPlanExtractor = function(plan, callback) {

	var propertiesHolder = [];

	var matcherNONJoinStrategyFunction = function(property) {
		return property == "left";
	};

	var exploreTreeNoJoinStrategyFunction = function(element, index, array) {
		whereTreeExplorer(element, propertiesHolder, matcherNONJoinStrategyFunction, exploreTreeNoJoinStrategyFunction);
	};

	whereTreeExplorer(plan.WHERE, propertiesHolder, matcherNONJoinStrategyFunction, exploreTreeNoJoinStrategyFunction);
	callback(propertiesHolder);
};

var propertyExtractionFromQueryPlan = function(plan, callback) {

	var propertiesHolder = [];

	var combineWithPropertiesHolder = function(array) {
		for ( var prop in array) {
			if (propertiesHolder[prop] === undefined)
				propertiesHolder[prop] = array[prop];
			else
				propertiesHolder[prop] = propertiesHolder[prop].concat(array[prop]);
		}
	};

	var selectFunction = function(data) {
		propertiesHolder = data;
		joinPropertyFromQueryPlanExtractor(plan, joinFunction);
	};

	var joinFunction = function(data) {
		combineWithPropertiesHolder(data);
		wherePropertyFromQueryPlanExtractor(plan, whereFunction);
	};
	var whereFunction = function(data) {
		combineWithPropertiesHolder(data);
		callback(propertiesHolder);
	};

	selectPropertyFromQueryPlanExtractor(plan, selectFunction);
};

module.exports.checkSchemaComplience = checkSchemaComplience;
module.exports.propertyExtractionFromQueryPlan = propertyExtractionFromQueryPlan;
module.exports.selectPropertyFromQueryPlanExtractor = selectPropertyFromQueryPlanExtractor;
module.exports.joinPropertyFromQueryPlanExtractor = joinPropertyFromQueryPlanExtractor;
module.exports.wherePropertyFromQueryPlanExtractor = wherePropertyFromQueryPlanExtractor;
