/**
 * This object wrapping around PHT interface
 */

function PHTWrapper(phtInterface) {
    var self = this instanceof PHTWrapper ? this : Object.create(PHTWrapper.prototype);
    self.phtInterface = phtInterface;
    return self;
}

PHTWrapper.prototype.pastNoIndTag = "PASS_NO_IND";
	
PHTWrapper.prototype.executeSearch = function(namespace,key,value,operation,callback){	
	var propertyTable = createPropertyTableName(namespace,key);
	switch(operation){
	case ">":
		this.phtInterface.smaller(propertyTable,value,callback);
		break;
	case ">=":
		this.phtInterface.smallerEq(propertyTable,value,callback);
		break;
	case "<":
		this.phtInterface.greater(propertyTable,value,callback);
		break;
	case "<=":
		this.phtInterface.greaterEq(propertyTable,value,callback);
		break;
	default:
		callback(null,[this.pastNoIndTag]);
	}
	
};

var  createPropertyTableName = function(namespace,key){
	return "index-" + namespace + "-" + key;
};

//EXPORT
module.exports.PHTWrapper = PHTWrapper;