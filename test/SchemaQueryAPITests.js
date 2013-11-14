/**
 * New node file
 */
var schemaQueryAPi = require('../lib/SchemaQueryAPI');
var extend = require('util')._extend;

var tableName = 'persons';

var columnId = {
	type : 'INT',
	name : 'id',
	notNull : true
};

var columnFirstName = {
	type : 'VARCHAR',
	name : 'fname'
};
var columnLastName = {
	type : 'VARCHAR',
	name : 'lname',
	notNull : false
};
var primaryKeyProperty = {
	type : 'CONSTRAINT',
	constraint : 'PRIMARY KEY',
	constraintName : 'pk_PersonID',
	columns : [ 'id' ]
};

var realSchema = [];
realSchema.table = tableName;
realSchema.primaryKey = 'id';
realSchema.indexes = [];
realSchema.properties = [];
realSchema.properties.id = {
	type : 'INT',
	notNull : true
};
realSchema.properties.fname = {
	type : 'VARCHAR',
	notNull : false
};
realSchema.properties.lname = {
	type : 'VARCHAR',
	notNull : false
};

var initialSchema = [];
initialSchema.table = tableName;
initialSchema.definitions = [];
initialSchema.definitions[0] = columnId;
initialSchema.definitions[1] = columnFirstName;
initialSchema.definitions[2] = columnLastName;
initialSchema.definitions[3] = primaryKeyProperty;

exports.schemaTransformation = {
	shouldTransformIntialPlanToRealOne : function(test) {
		schemaQueryAPi.permenentSchemaCreateror(initialSchema, function(err, schema) {
			test.equal(err, undefined, "Should not produce error");
			test.deepEqual(realSchema, schema, "Schema did not match");
			test.done();
		});
	},
	
	shouldCrashWhenNoSchemaIsProvided : function(test) {
		schemaQueryAPi.permenentSchemaCreateror(undefined, function(err, schema) {
			test.notEqual(err, undefined, "Should throw error if schema is not defined.");
			test.equal(undefined, schema, "Should not produce real schema.");
			test.done();
		});
	},

	shouldNotCrashWhenNameSpaceIsMissing : function(test) {
		var tempInitSchema = extend([], initialSchema);
		tempInitSchema.table = undefined;

		var tempRealSchema = extend([], realSchema);
		tempRealSchema.table = undefined;

		schemaQueryAPi.permenentSchemaCreateror(tempInitSchema, function(err, schema) {
			test.equal(err, undefined, "Should not throw error about missing namespace.");
			test.deepEqual(tempRealSchema, schema, "Should produce schema without namespace.");
			test.done();
		});
	},
	
	shouldNotCrashWhenThereAreNoProperties : function(test) {
		var tempInitSchema = extend([], initialSchema);
		tempInitSchema.definitions = undefined;

		var tempRealSchema = extend([], realSchema);
		tempRealSchema.properties = undefined;
		tempRealSchema.primaryKey = undefined;
		
		schemaQueryAPi.permenentSchemaCreateror(tempInitSchema, function(err, schema) {
			test.equal(err, undefined, "Should not throw error about missing properties.");
			test.deepEqual(tempRealSchema, schema, "Should produce schema without properties.");
			test.done();
		});
	}
	
	
};
