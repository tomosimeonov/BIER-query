/**
 * SQL parser for decoding SQL expressions, based on several open sourced
 * parsers.
 */

var sqljs = require('sqljs');

var options = new sqljs.ParseOptions;

function createTableParser(text) {
	try {
		var result = sqljs.parse(text, undefined, options);
		return result;
	} catch (err) {
		return err;
	}
}

function createIndexParser(text) {
	var indexValues = text.substring(13).split(' ');
	return {
		"table" : indexValues[2],
		"property" : indexValues[0]
	};
}

var selectParser = require('simple-sql-parser');

var bierSqlParser = function(sql, callback) {
	if (sql.slice(0, 6).toLowerCase() == 'create')
		if (sql.slice(7, 12).toLowerCase() == 'table')
			callback(createTableParser(sql));
		else
			callback(createIndexParser(sql));
	else
		callback(selectParser(sql));
};

module.exports = bierSqlParser;