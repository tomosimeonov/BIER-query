/**
 * New node file
 */

var permenentSchemaCreateror = function(initialSchema, callback) {

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
		var modifiedSchema = {};
		modifiedSchema.table = initialSchema.table;
		modifiedSchema.properties = {};
		initialSchema.definitions.forEach(function(element, index, array) {
			if (element.name) {
				var notNull = false;
				if (element.notNull)
					notNull = element.notNull;
				modifiedSchema.properties[element.name] = {
					'type' : transformType(element.type),
					'notNull' : notNull
				};
			}
			if (element.constraint && element.constraint === 'PRIMARY KEY') {
				modifiedSchema.primaryKey = element.columns[0];
				
			}
		});
		modifiedSchema.indexes = {};

		callback(undefined, modifiedSchema);
	} catch (err) {
		callback(err, undefined);
	}
};

var isSatisfingSchemaStructure = function(schemaToCheck, callback) {

	var checkProp = function(properties) {
		var answer = true;
		for (key in properties) {
			if (properties.hasOwnProperty(key)
					&& (properties[key].type === undefined || properties[key].type === "" || properties[key].notNull === undefined))
				answer = false;
		}
		return answer;
	};

	if (schemaToCheck === undefined || schemaToCheck === "")
		callback(new Error("No schema provided."), false);
	else if (schemaToCheck.table === undefined || schemaToCheck.table === "")
		callback(new Error("No namespace provided"), false);
	else if (schemaToCheck.properties === undefined || schemaToCheck.properties.size === 0)
		callback(new Error("No properties provided."), false);
	else if (!checkProp(schemaToCheck.properties))
		callback(new Error("Property with wrong format."), false);
	else if (schemaToCheck.primaryKey === undefined
			|| !schemaToCheck.properties.hasOwnProperty(schemaToCheck.primaryKey))
		callback(new Error("No primary key or primary key not specified as property"), false);
	else if (schemaToCheck.indexes === undefined)
		callback(new Error("Index element should be present even if there are no indexes"), false);
	else
		callback(undefined, true);
};

var isSatisfingSchema = function(propertiesToCheck, tableSchema, callback) {
	try {
		var notFoundProp = propertiesToCheck.length;
		if (tableSchema === undefined || tableSchema.properties === undefined)
			throw new Error("Schema with wrong format!");
		for (key in tableSchema.properties) {
			if (tableSchema.properties.hasOwnProperty(key) && propertiesToCheck.indexOf(key) > -1)
				notFoundProp--;
		}
		callback(undefined, notFoundProp === 0);
	} catch (err) {
		callback(err, true);
	}

};

var transformStoredEntryToSchemaValid = function(schema, data, callback){
	
	
	var propKeys = Object.keys(schema.properties);
	
	for(var key in propKeys){
		if(schema.properties.type === "int"){
			for(var i = 0; i < data.size(); i ++){
				data[i][key] = parseInt(data[i][key]);
			}

		}
		if(schema.properties.type === "float"){
			for(var i = 0; i < data.size(); i ++){
				data[i][key] = parseFloat(data[i][key]);
			}
		}
	}
	
	callback(data);
};

module.exports.isSatisfingSchema = isSatisfingSchema;
module.exports.isSatisfingSchemaStructure = isSatisfingSchemaStructure;
module.exports.permenentSchemaCreateror = permenentSchemaCreateror;
module.exports.transformStoredEntryToSchemaValid = transformStoredEntryToSchemaValid;