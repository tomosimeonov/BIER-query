/**
 * SQL parser for decoding SQL expressions, based on several open sourced
 * parsers.
 * 
 * @author Tomo Simeonov
 */

var sqljs = require('sqljs');
var selectParser = require('simple-sql-parser');

var options = new sqljs.ParseOptions;

//Create parser for table
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

//Creates parser for indexes
function createIndexParser(text, callback) {
	var indexValues = text.substring(13).split(' ');
	callback(null, {
		'createIndex' : {
			"table" : indexValues[2],
			"property" : indexValues[0]
		}
	});
}

//Extract and remove timeouts
function separateTimeout(sql,callback){
	
	var sqlSplitted = sql.split("TIMEOUT ");
	var selectedParsed = selectParser(sqlSplitted[0]);
	if(sqlSplitted[1] !== undefined){
		var timeout = sqlSplitted[1].split("=");
		if(timeout[0].indexOf("Time") != -1){
			selectedParsed.TIMEOUT = {};
			selectedParsed.TIMEOUT.time =  parseInt(timeout[1]);
		}else if(timeout[0].indexOf("Objects") != -1){
			selectedParsed.TIMEOUT = {};
			selectedParsed.TIMEOUT.objects =  parseInt(timeout[1]);
		}
	}
	callback(undefined,selectedParsed);
	
}


/**
 * Function to parse SQL string statement into SQL plan.
 */
var bierSqlParser = function(sql, callback) {
	if (sql.slice(0, 6).toLowerCase() == 'create')
		if (sql.slice(7, 12).toLowerCase() == 'table')
			createTableParser(sql, callback);
		else
			createIndexParser(sql, callback);
	else
		separateTimeout(sql,callback);
};

module.exports = bierSqlParser;