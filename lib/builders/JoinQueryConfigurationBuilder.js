/**
 * A builder to help build initial configuration for join query execution.
 */
function JoinQueryConfigurationBuilder() {
	var self = this instanceof JoinQueryConfigurationBuilder ? this : Object
			.create(JoinQueryConfigurationBuilder.prototype);
	self.build = {};
	self.build.properties = [];
	self.build.namespaceOne = "";
	self.build.namespaceTwo = "";
	self.build.type = this.NONE;
	self.build.joinPropertyOne = "";
	self.build.joinPropertyTwo = "";
	self.build.filterPlan = {};
	self.build.maxObjects = -1;
	self.build.timeout = undefined;
	return self;
};

JoinQueryConfigurationBuilder.prototype.NONE = 0;
JoinQueryConfigurationBuilder.prototype.ONE = 1;
JoinQueryConfigurationBuilder.prototype.TWO = 2;
JoinQueryConfigurationBuilder.prototype.BOTH = 3;

JoinQueryConfigurationBuilder.prototype.buildQueryConfig = function() {
	if (this.build.namespaceOne === "" || this.build.namespaceTwo === "") {
		return undefined;
	} else {
		if (this.TWO === this.build.type) {
			var temp = this.build.joinPropertyOne;
			build.joinPropertyOne = build.joinPropertyTwo;
			build.joinPropertyTwo = temp;

			temp = this.build.namespaceOne;
			build.namespaceOne = build.namespaceTwo;
			build.namespaceTwo = temp;
		}
		return this.build;
	}
};

JoinQueryConfigurationBuilder.prototype.setNamespaceOne = function(namespace) {
	this.build.namespaceOne = namespace;
	return this;
};

JoinQueryConfigurationBuilder.prototype.setNamespaceTwo = function(namespace) {
	this.build.namespaceTwo = namespace;
	return this;
};

JoinQueryConfigurationBuilder.prototype.setFullType = function(type) {
	if (type !== this.NONE && type !== this.ONE && type !== this.TWO && type !== this.BOTH)
		this.build.type = this.NONE;
	else
		this.build.type = type;
	return this;
};

JoinQueryConfigurationBuilder.prototype.setJoinPropertyOne = function(joinProp) {
	this.build.joinPropertyOne = joinProp;
	return this;
};

JoinQueryConfigurationBuilder.prototype.setJoinPropertyTwo = function(joinProp) {
	this.build.joinPropertyTwo = joinProp;
	return this;
};

JoinQueryConfigurationBuilder.prototype.setFormattedProperties = function(formattedProperties) {
	this.build.properties = formattedProperties;
	return this;
};

JoinQueryConfigurationBuilder.prototype.setFilterPlan = function(filterPlan) {
	if (filterPlan !== undefined)
		this.build.filterPlan = filterPlan;
	return this;
};

JoinQueryConfigurationBuilder.prototype.setMaxObjects = function(maxObjects) {
	this.build.maxObjects = maxObjects;
	return this;
};

JoinQueryConfigurationBuilder.prototype.setTimeout = function(timeout) {
	this.build.timeout = timeout;
	return this;
};

module.exports.JoinQueryConfigurationBuilder = JoinQueryConfigurationBuilder;