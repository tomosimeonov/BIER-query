/**
 * Methods used to manipulate schemas.
 * 
 * @author Tomo Simeonov
 */

/**
 * Transforms sql schema into the format recognized by the database.
 */
var permenentSchemaCreateror = function(initialSchema, callback) {

	var transformToInternalType = function(typ) { 
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
					'type' : transformToInternalType(element.type),
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

/**
 * Verifies that given schema satisfies the internal format for schemas.
 */
var isSatisfingSchemaStructure = function(SchemaToCheck, callback) {

	var checkProp = function(properties) {
		var answer = true;
		for (key in properties) {
			if (properties.hasOwnProperty(key)
					&& (properties[key].type === undefined || properties[key].type === "" || properties[key].notNull === undefined))
				answer = false;
		}
		return answer;
	};

	if (SchemaToCheck === undefined || SchemaToCheck === "")
		callback(new Error("No Schema provided."), false);
	else if (SchemaToCheck.table === undefined || SchemaToCheck.table === "")
		callback(new Error("No namespace provided"), false);
	else if (SchemaToCheck.properties === undefined || SchemaToCheck.properties.size === 0)
		callback(new Error("No properties provided."), false);
	else if (!checkProp(SchemaToCheck.properties))
		callback(new Error("Property with wrong format."), false);
	else if (SchemaToCheck.primaryKey === undefined
			|| !SchemaToCheck.properties.hasOwnProperty(SchemaToCheck.primaryKey))
		callback(new Error("No primary key or primary key not specified as property"), false);
	else if (SchemaToCheck.indexes === undefined)
		callback(new Error("Index element should be present even if there are no indexes"), false);
	else
		callback(undefined, true);
};

/**
 * Checks if given properties satisfy a give schema.
 */
var isSatisfingSchema = function(propertiesToCheck, tableSchemas, callback) {
	try {
		var result = true;
		for(var key in propertiesToCheck){
			var notFoundProp = propertiesToCheck[key].length;
			if (tableSchemas[key] === undefined || tableSchemas[key].properties === undefined)
				if(key === 'default'){
					throw new Error("In join select using property without saying which table.");
				}else{
					throw new Error("Schema with wrong format or missing: " + key);
				}
			for (key1 in tableSchemas[key].properties) {
				if (propertiesToCheck[key].indexOf(key1) > -1)
					notFoundProp--;
			}
			result = result && (notFoundProp === 0);
			
			if(result === false){
				continue;
			}
		}
		
		callback(undefined, result);
	} catch (err) {
		callback(err, false);
	}

};

module.exports.isSatisfingSchema = isSatisfingSchema;
module.exports.isSatisfingSchemaStructure = isSatisfingSchemaStructure;
module.exports.permenentSchemaCreateror = permenentSchemaCreateror;