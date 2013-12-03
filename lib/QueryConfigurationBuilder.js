/**
 * A builder to help build initial configuration for query execution, for simple queries.
 */
function QueryConfigurationBuilder() {
	var self = this instanceof QueryConfigurationBuilder ? this : Object.create(QueryConfigurationBuilder.prototype);
	self.build = {};
	self.build.properties = [];
	self.build.aggregations = [];
	self.build.namespace = "";
	self.build.filterPlan = {};
	self.build.maxObjects = -1;
	self.build.timeout = undefined;
	self.build.destinations = undefined;
	return self;
};

QueryConfigurationBuilder.prototype.buildQueryConfig = function() {
	if (this.build.namespace === "" || this.build.destinations === undefined) {
		return undefined;
	} else {
		return this.build;
	}
};

QueryConfigurationBuilder.prototype.setNamespace = function(namespace) {
	this.build.namespace = namespace;
	return this;
};

QueryConfigurationBuilder.prototype.setDestinations = function(destinations) {
	this.build.destinations = destinations;
	return this;
};

QueryConfigurationBuilder.prototype.setFormattedProperties = function(formattedProperties) {
	this.build.aggregations = formattedProperties.aggreg;
	this.build.properties = formattedProperties.prop;
	return this;
};

QueryConfigurationBuilder.prototype.setFilterPlan = function(filterPlan) {
	this.build.filterPlan = filterPlan;
	return this;
};

QueryConfigurationBuilder.prototype.setMaxObjects = function(maxObjects) {
	this.build.maxObjects = maxObjects;
	return this;
};

QueryConfigurationBuilder.prototype.setTimeout = function(timeout) {
	this.build.timeout = timeout;
	return this;
};

module.exports.QueryConfigurationBuilder = QueryConfigurationBuilder;