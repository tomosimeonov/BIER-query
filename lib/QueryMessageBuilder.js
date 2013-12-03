/**
 * Builder to build messages to be send to other nodes.
 */
function QueryMessageBuilder() {
	var self = this instanceof QueryMessageBuilder ? this : Object.create(QueryMessageBuilder.prototype);
	self.build = {};
	self.build.queryId = "";
	self.build.type = "";
	self.build.payload = {};
	self.build.origin = "";
	return self;
};

QueryMessageBuilder.prototype.SIMPLE_TYPE = "simple";
QueryMessageBuilder.prototype.JOIN_TYPE = "join";

QueryMessageBuilder.prototype.buildMessage = function() {
	if (this.build.queryId === "" || this.build.payload.length === 0 || this.build.type === ""
			|| this.build.origin === "") {
		return undefined;
	} else {
		return this.build;
	}
};

QueryMessageBuilder.prototype.setSimpleType = function() {
	this.build.type = this.SIMPLE_TYPE;
	return this;
};

QueryMessageBuilder.prototype.setJoinType = function() {
	this.build.type = this.JOIN_TYPE;
	return this;
};

QueryMessageBuilder.prototype.setPayload = function(payload) {
	this.build.payload = payload;
	return this;
};

QueryMessageBuilder.prototype.setQueryId = function(queryId) {
	this.build.queryId = queryId;
	return this;
};

QueryMessageBuilder.prototype.setOrigin = function(origin) {
	this.build.origin = origin;
	return this;
};

module.exports.QueryMessageBuilder = QueryMessageBuilder;