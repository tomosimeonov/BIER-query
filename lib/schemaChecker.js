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

var propertyExtractionFromQueryPlan = function(plan, callback) {

	var propertiesHolder = [];

	var updateHolder = function(table, property) {
		if (propertiesHolder[table] === undefined)
			propertiesHolder[table] = [ property ];
		else if (propertiesHolder[table].indexOf(property) == -1)
			propertiesHolder[table].push(property);
	};
	var insertPropertyIntoHolder = function(property) {
		if (property.indexOf('.') > -1) {
			var tableSpecific = property.split('.');
			updateHolder(tableSpecific[0], tableSpecific[1]);
		} else if (property != '*')
			updateHolder('default', property);
	};

	var forEachWrapperForInsert = function(element, index, array) {
		insertPropertyIntoHolder(element);
	};

	var matcherNONJoinStrategyFunction = function(property) {
		return property == "left";
	};
	var matcherJoinStrategyFunction = function(property) {
		return property == "left" || property == "right";
	};

	var exploreTreeNoJoinStrategyFunction = function(element, index, array) {
		whereTreeExplorer(element, matcherNONJoinStrategyFunction, exploreTreeNoJoinStrategyFunction);
	};
	var exploreTreeJoinStrategyFunction = function(element, index, array) {
		whereTreeExplorer(element, matcherJoinStrategyFunction, exploreTreeJoinStrategyFunction);
	};

	var whereTreeExplorer = function(whereProperties, matcherStrategyFunction, exploreTreeStartegyFunction) {
		for ( var property in whereProperties) {
			if (property == "terms")
				whereProperties[property].forEach(exploreTreeStartegyFunction);
			if (matcherStrategyFunction(property))
				insertPropertyIntoHolder(whereProperties[property]);
		}
	};

	var selectPropertyExtraction = function(callback) {
		plan.SELECT.forEach(forEachWrapperForInsert);
		joinPropertyExtraction(callback);
	};

	var joinPropertyExtraction = function(callback) {
		if (plan['INNER JOIN'] !== undefined) {
			whereTreeExplorer(plan['INNER JOIN'].cond, matcherJoinStrategyFunction, exploreTreeJoinStrategyFunction);
		} else if (plan['LEFT JOIN'] !== undefined) {
			whereTreeExplorer(plan['LEFT JOIN'].cond, matcherJoinStrategyFunction, exploreTreeJoinStrategyFunction);
		} else if (plan['RIGHT JOIN'] !== undefined) {
			whereTreeExplorer(plan['RIGHT JOIN'].cond, matcherJoinStrategyFunction, exploreTreeJoinStrategyFunction);
		}
		wherePropertyExtraction(callback);
	};

	var wherePropertyExtraction = function(callback) {
		whereTreeExplorer(plan.WHERE, matcherNONJoinStrategyFunction, exploreTreeNoJoinStrategyFunction);
		callback(propertiesHolder);
	};

	var extractProperties = function() {
		selectPropertyExtraction(callback);
	};

	extractProperties();
};

module.exports.checkSchemaComplience = checkSchemaComplience;
module.exports.propertyExtractionFromQueryPlan = propertyExtractionFromQueryPlan;