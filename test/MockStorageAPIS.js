/**
 * Mocking storage layer apis
 */

function MockStorageAPIS() {
	var self = this instanceof MockStorageAPIS ? this : Object.create(MockStorageAPIS.prototype);
	self.Node = [];
	self.Node.lscan= undefined;
	self.Node.node = [];
	self.Node.node.getID = function(){return "test";};
	self.Node.send = function(a,b,c){};
	self.Node.broadcast = function(a,b,c){};
	return self;
}

MockStorageAPIS.prototype.setLscan = function(lscanDataFunction){
	this.Node.lscan = lscanDataFunction;
};

MockStorageAPIS.prototype.setBroadcast = function(broadcastDataFunction){
	this.Node.broadcast = broadcastDataFunction;
};

MockStorageAPIS.prototype.setDefaultBroadcast = function(broadcastDataFunction){
	this.Node.broadcast = function(a,b,c){};
};

//MockStorageAPIS.prototype.lscan = function(namespace){
//	return this.returnLscan(namespace);
//};

module.exports.MockStorageAPIS = MockStorageAPIS;