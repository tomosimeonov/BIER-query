/**
 * Builder to build messages to be send to other nodes.
 */
function QueryMessageBuilder() {
	var self = this instanceof QueryMessageBuilder ? this : Object.create(QueryMessageBuilder.prototype);
	self.build = {};
	self.build.queryId = "";
	self.build.type = "";
	self.build.payload = undefined;
	self.build.origin = "";
	self.build.payloadType = "";
	return self;
};

SIMPLE_TYPE = "simple";
JOIN_TYPE = "join";
PAYLOAD_TYPE_CONFIG = "CONFIG";
PAYLOAD_TYPE_DATA = "DATA";

QueryMessageBuilder.prototype.buildMessage = function() {
	if (this.build.queryId === "" || this.build.payload === undefined || this.build.type === ""
			|| this.build.origin === "") {
		return undefined;
	} else {
		return this.build;
	}
};

QueryMessageBuilder.prototype.setSimpleType = function() {
	this.build.type = SIMPLE_TYPE;
	return this;
};

QueryMessageBuilder.prototype.setJoinType = function() {
	this.build.type = JOIN_TYPE;
	return this;
};

QueryMessageBuilder.prototype.setPayload = function(payload) {
	this.build.payload = payload;
	return this;
};

QueryMessageBuilder.prototype.setCustomPayloadType = function(type) {
	this.build.payloadType = type;
	return this;
};

QueryMessageBuilder.prototype.setPayloadTypeConfig = function() {
	this.build.payloadType = PAYLOAD_TYPE_CONFIG;
	return this;
};

QueryMessageBuilder.prototype.setPayloadTypeData = function() {
	this.build.payloadType = PAYLOAD_TYPE_DATA;
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
module.exports.SIMPLE_TYPE = SIMPLE_TYPE;
module.exports.JOIN_TYPE = JOIN_TYPE;
module.exports.PAYLOAD_TYPE_DATA = PAYLOAD_TYPE_DATA;
module.exports.PAYLOAD_TYPE_CONFIG = PAYLOAD_TYPE_CONFIG;