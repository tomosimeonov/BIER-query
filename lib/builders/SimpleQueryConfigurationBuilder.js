/**
 * A builder to help build initial configuration for query execution, for simple queries.
 * 
 * @author Tomo Simeonov
 */
function SimpleQueryConfigurationBuilder() {
	var self = this instanceof SimpleQueryConfigurationBuilder ? this : Object.create(SimpleQueryConfigurationBuilder.prototype);
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

SimpleQueryConfigurationBuilder.prototype.init = function(){
	this.build.properties = [];
	this.build.aggregations = [];
	this.build.namespace = "";
	this.build.filterPlan = {};
	this.build.maxObjects = -1;
	this.build.timeout = undefined;
	this.build.destinations = undefined;
	return this;
};

SimpleQueryConfigurationBuilder.prototype.buildQueryConfig = function() {
	if (this.build.namespace === "" || this.build.destinations === undefined) {
		return undefined;
	} else {
		return this.build;
	}
};

SimpleQueryConfigurationBuilder.prototype.setNamespace = function(namespace) {
	this.build.namespace = namespace;
	return this;
};

SimpleQueryConfigurationBuilder.prototype.setDestinations = function(destinations) {
	this.build.destinations = destinations;
	return this;
};

SimpleQueryConfigurationBuilder.prototype.setFormattedProperties = function(formattedProperties) {
	this.build.aggregations = formattedProperties.aggreg;
	this.build.properties = formattedProperties.prop;
	return this;
};

SimpleQueryConfigurationBuilder.prototype.setFilterPlan = function(filterPlan) {
	this.build.filterPlan = filterPlan;
	return this;
};

SimpleQueryConfigurationBuilder.prototype.setMaxObjects = function(maxObjects) {
	this.build.maxObjects = maxObjects;
	return this;
};

SimpleQueryConfigurationBuilder.prototype.setTimeout = function(timeout) {
	this.build.timeout = timeout;
	return this;
};

module.exports.SimpleQueryConfigurationBuilder = SimpleQueryConfigurationBuilder;