/**
 * New node file
 */
function QueryStorageBuilder() {
	var self = this instanceof QueryStorageBuilder ? this : Object.create(QueryStorageBuilder.prototype);
	self.build = {};
	self.build.properties = [];
	self.build.aggregations = [];
	self.build.namespace = "";
	self.build.filterPlan = {};
	self.build.maxObjects = undefined;
	self.build.timeout = undefined;
	return self;
};

QueryStorageBuilder.prototype.buildQueryConfig = function() {
	if (this.build.namespace === "") {
		return undefined;
	} else {
		return this.build;
	}
};

QueryStorageBuilder.prototype.setNamespace = function(namespace) {
	this.build.namespace = namespace;
	return this;
};

QueryStorageBuilder.prototype.setFormattedProperties = function(formattedProperties) {
	this.build.aggregations = formattedProperties.aggreg;
	this.build.properties = formattedProperties.prop;
	return this;
};

QueryStorageBuilder.prototype.setFilterPlan = function(filterPlan) {
	this.build.filterPlan = filterPlan;
	return this;
};

QueryStorageBuilder.prototype.setMaxObjects = function(maxObjects) {
	this.build.maxObjects = maxObjects;
	return this;
};

QueryStorageBuilder.prototype.setTimeout = function(timeout) {
	this.build.timeout = timeout;
	return this;
};

module.exports.QueryStorageBuilder = QueryStorageBuilder;