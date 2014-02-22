/**
 * New node file
 */

var permenentSchemeCreateror = function(initialScheme, callback) {

	var transformType = function(typ) { 
		var endSlice = typ.indexOf('(');
		if(endSlice === -1) endSlice = typ.size;
		var modifiedTyp = typ.slice(0,endSlice).toLowerCase();
		var integers = [ 'integer', 'smallint','int', 'numeric',  'bigint'];
		var floats = ['real','float', 'double', 'decimal'];
		var string = [ 'character', 'varchar' ];
		var boolean = [ 'boolean' ];

		if (integers.indexOf(modifiedTyp) > -1)
			return "int";
		if (floats.indexOf(modifiedTyp) > -1)
			return "floats";
		if (string.indexOf(modifiedTyp) > -1)
			return typeof "t";
		if (boolean.indexOf(modifiedTyp) > -1)
			return typeof true;
		return typeof [];
	};

	try {
		var modifiedScheme = {};
		modifiedScheme.table = initialScheme.table;
		modifiedScheme.properties = {};
		initialScheme.definitions.forEach(function(element, index, array) {
			if (element.name) {
				var notNull = false;
				if (element.notNull)
					notNull = element.notNull;
				modifiedScheme.properties[element.name] = {
					'type' : transformType(element.type),
					'notNull' : notNull
				};
			}
			if (element.constraint && element.constraint === 'PRIMARY KEY') {
				modifiedScheme.primaryKey = element.columns[0];
				
			}
		});
		modifiedScheme.indexes = {};

		callback(undefined, modifiedScheme);
	} catch (err) {
		callback(err, undefined);
	}
};

var isSatisfingSchemeStructure = function(SchemeToCheck, callback) {

	var checkProp = function(properties) {
		var answer = true;
		for (key in properties) {
			if (properties.hasOwnProperty(key)
					&& (properties[key].type === undefined || properties[key].type === "" || properties[key].notNull === undefined))
				answer = false;
		}
		return answer;
	};

	if (SchemeToCheck === undefined || SchemeToCheck === "")
		callback(new Error("No Scheme provided."), false);
	else if (SchemeToCheck.table === undefined || SchemeToCheck.table === "")
		callback(new Error("No namespace provided"), false);
	else if (SchemeToCheck.properties === undefined || SchemeToCheck.properties.size === 0)
		callback(new Error("No properties provided."), false);
	else if (!checkProp(SchemeToCheck.properties))
		callback(new Error("Property with wrong format."), false);
	else if (SchemeToCheck.primaryKey === undefined
			|| !SchemeToCheck.properties.hasOwnProperty(SchemeToCheck.primaryKey))
		callback(new Error("No primary key or primary key not specified as property"), false);
	else if (SchemeToCheck.indexes === undefined)
		callback(new Error("Index element should be present even if there are no indexes"), false);
	else
		callback(undefined, true);
};

var isSatisfingScheme = function(propertiesToCheck, tableScheme, callback) {
	try {
		var notFoundProp = propertiesToCheck.length;
		if (tableScheme === undefined || tableScheme.properties === undefined)
			throw new Error("Scheme with wrong format!");
		for (key in tableScheme.properties) {
			if (tableScheme.properties.hasOwnProperty(key) && propertiesToCheck.indexOf(key) > -1)
				notFoundProp--;
		}
		callback(undefined, notFoundProp === 0);
	} catch (err) {
		callback(err, true);
	}

};

module.exports.isSatisfingScheme = isSatisfingScheme;
module.exports.isSatisfingSchemeStructure = isSatisfingSchemeStructure;
module.exports.permenentSchemeCreateror = permenentSchemeCreateror;