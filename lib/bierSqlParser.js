/**
 * SQL parser for decoding SQL expressions, based on several open sourced
 * parsers.
 */

var sqljs = require('sqljs');

var options = new sqljs.ParseOptions;

function createTableParser(text, callback) {
	try {
		var partialResult = sqljs.parse(text, undefined, options)[0];
		delete partialResult.statement;
		delete partialResult.what;
		delete partialResult.schema;
		callback(null, partialResult);
	} catch (err) {
		callback(err, null);
	}
}

function createIndexParser(text, callback) {
	var indexValues = text.substring(13).split(' ');
	callback(null, {
		'createIndex' : {
			"table" : indexValues[2],
			"property" : indexValues[0]
		}
	});
}

var selectParser = require('simple-sql-parser');

var bierSqlParser = function(sql, callback) {
	if (sql.slice(0, 6).toLowerCase() == 'create')
		if (sql.slice(7, 12).toLowerCase() == 'table')
			createTableParser(sql, callback);
		else
			createIndexParser(sql, callback);
	else
		callback(null,selectParser(sql));
};

module.exports = bierSqlParser;