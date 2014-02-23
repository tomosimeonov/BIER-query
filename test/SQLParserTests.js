/**
 * New node file
 */
var parser = new require('../lib/SqlParser');

var tableSQL = "CREATE TABLE Persons(P_Id int NOT NULL, LastName varchar(255) NOT NULL, FirstName varchar(255), "
		+ "Address varchar(255), City varchar(255), PRIMARY KEY (P_Id))";

var insertNoTime = "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country, KEEP_ALIVE) "
		+ "VALUES ('Cardinal','Tom B. Erichsen','Skagen 21','Stavanger','4006','Norway','2');";
var insertWTime = "INSERT INTO Customers (CustomerName, ContactName, Address, City, PostalCode, Country, KEEP_ALIVE) "
		+ "VALUES ('Cardinal','Tom B. Erichsen','Skagen 21','Stavanger','4006','Norway','2') TIMEOUT Time=5;";

var simpleSelectNoTime = "SELECT * FROM Orders WHERE id = 5";

var joinSelectNoTime = "SELECT * FROM Orders INNER JOIN Customers ON Orders.CustomerName=Customers.CustomerName;";

var simpleSelectWTime = "SELECT * FROM Orders WHERE id = 5 TIMEOUT Objects=5";

var joinSelectWTime = "SELECT * FROM Orders INNER JOIN Customers ON Orders.CustomerName=Customers.CustomerName "
		+ "TIMEOUT Time=5;";

var tableSQLAnswer = {
	table : 'Persons',
	definitions : [ {
		notNull : true,
		type : 'INT',
		name : 'P_Id'
	}, {
		notNull : true,
		type : 'VARCHAR',
		length : 255,
		name : 'LastName'
	}, {
		type : 'VARCHAR',
		length : 255,
		name : 'FirstName'
	}, {
		type : 'VARCHAR',
		length : 255,
		name : 'Address'
	}, {
		type : 'VARCHAR',
		length : 255,
		name : 'City'
	}, {
		type : 'CONSTRAINT',
		constraint : 'PRIMARY KEY',
		constraintName : undefined,
		columns : [ 'P_Id' ]
	} ]
};

var insertAnswerNoTime = {
	'INSERT INTO' : {
		table : 'Customers',
		columns : [ 'CustomerName', 'ContactName', 'Address', 'City', 'PostalCode', 'Country', 'KEEP_ALIVE' ]
	},
	VALUES : [ [ '\'Cardinal\'', '\'Tom B. Erichsen\'', '\'Skagen 21\'', '\'Stavanger\'', '\'4006\'', '\'Norway\'',
			'\'2\'' ] ]
};

var insertAnswerWTime = {
	'INSERT INTO' : {
		table : 'Customers',
		columns : [ 'CustomerName', 'ContactName', 'Address', 'City', 'PostalCode', 'Country', 'KEEP_ALIVE' ]
	},
	VALUES : [ [ '\'Cardinal\'', '\'Tom B. Erichsen\'', '\'Skagen 21\'', '\'Stavanger\'', '\'4006\'', '\'Norway\'',
			'\'2\'' ] ],
	TIMEOUT : {
		time : 5
	}
};

var simpleSelectAnswer = {
	SELECT : [ '*' ],
	FROM : [ 'Orders' ],
	WHERE : {
		operator : '=',
		left : 'id',
		right : '5'
	}
};

var joinSelectAnswer = {
	SELECT : [ '*' ],
	FROM : [ 'Orders' ],
	'INNER JOIN' : {
		table : 'Customers',
		cond : {
			operator : '=',
			left : 'Orders.CustomerName',
			right : 'Customers.CustomerName'
		}
	}
};

exports.simpleTests = {
	shouldParseInsertWithoutTimeout : function(test) {

		parser(insertNoTime, function(err, data) {
			test.deepEqual(insertAnswerNoTime, data, 'Should not match data.');
			test.done();
		});
	},
	shouldParseInsertWithTimeout : function(test) {
		parser(insertWTime, function(err, data) {
			test.deepEqual(insertAnswerWTime, data, 'Should not match data.');
			test.done();
		});
	},
	shouldParseTableInsertSQL : function(test) {
		parser(tableSQL, function(err, data) {
			test.deepEqual(tableSQLAnswer, data, 'Should not match data.');
			test.done();
		});
	},
	shouldParseSimpleSQLNoTime : function(test) {
		parser(simpleSelectNoTime, function(err, data) {
			test.deepEqual(simpleSelectAnswer, data, 'Should not match data.');
			test.done();
		});
	},
	shouldParseJoinSQLNoTime : function(test) {
		parser(joinSelectNoTime, function(err, data) {
			test.deepEqual(joinSelectAnswer, data, 'Should not match data.');
			test.done();
		});
	},
	shouldParseSimpleSQLNoTime : function(test) {
		var answer = simpleSelectAnswer;
		
		parser(simpleSelectWTime, function(err, data) {
			answer.TIMEOUT = {};
			answer.TIMEOUT.objects = 5;
			test.deepEqual(simpleSelectAnswer, data, 'Should not match data.');
			test.done();
		});
	},
	shouldParseJoinSQLNoTime : function(test) {
		var answer = joinSelectAnswer;
		
		parser(joinSelectWTime, function(err, data) {
			answer.TIMEOUT = {};
			answer.TIMEOUT.time = 5;
			test.deepEqual(joinSelectAnswer, data, 'Should not match data.');
			test.done();
		});
	},
};