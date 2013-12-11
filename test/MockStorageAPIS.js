/**
 * Mocking storage layer apis
 */

function MockStorageAPIS() {
	var self = this instanceof MockStorageAPIS ? this : Object.create(MockStorageAPIS.prototype);
	self.Node = [];
	self.Node.lscan= undefined;
	return self;
}

MockStorageAPIS.prototype.setLscan = function(lscanDataFunction){
	this.Node.lscan = lscanDataFunction;
};

//MockStorageAPIS.prototype.lscan = function(namespace){
//	return this.returnLscan(namespace);
//};

module.exports.MockStorageAPIS = MockStorageAPIS;