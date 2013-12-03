/**
 * Builder to build messages to be send to other nodes.
 */
function QueryMessageBuilder() {
	var self = this instanceof QueryMessageBuilder ? this : Object.create(QueryMessageBuilder.prototype);
	self.build = {};
	self.build.destination = "";
	self.build.type = "";
	self.build.payload = {};
	return self;
};

QueryMessageBuilder.prototype.SIMPLE_TYPE = "simple";
QueryMessageBuilder.prototype.JOIN_TYPE = "join";



QueryMessageBuilder.prototype.buildMessage = function() {
	if (this.build.payload.length === 0 || this.build.type === "" ) {
		return undefined;
	} else {
		return this.build;
	}
};

QueryMessageBuilder.prototype.setDestination = function(destination) {
	this.build.destination = destination;
	return this;
};

QueryMessageBuilder.prototype.setSimpleType = function() {
	this.build.type = this.SIMPLE_TYPE;
	return this;
};

QueryMessageBuilder.prototype.setJoinType = function() {
	this.build.type = this.JOIN_TYPE;
	return this;
};

QueryMessageBuilder.prototype.setPayload= function(payload) {
	this.build.payload = payload;
	return this;
};

module.exports.QueryMessageBuilder = QueryMessageBuilder;