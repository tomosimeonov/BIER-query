/**
 * Common constants
 * 
 * @author Tomo Simeonov
 */

// For aggregation
module.exports.MAX_AGGREGATION = 'MAX';
module.exports.MIN_AGGREGATION = 'MIN';
module.exports.COUNT_AGGREGATION = 'COUNT';
module.exports.SUM_AGGREGATION = 'SUM';
module.exports.AVR_AGGREGATION = 'AVR';

// For logging etc.
module.exports.NEW_QUERY = "NEW_QUERY";
module.exports.FIN_QUERY = "QUERY_FINISHED";
module.exports.QUERY_PERF = "PERFORMANCE";
module.exports.LOG_INFO = "LOG_INFO";
module.exports.LOG_FINE = "LOG_FINE";
module.exports.LOG_FINER = "LOG_FINER";
module.exports.RUNNING_QUERIES = "RUNNING_QUERIES";

// Emiter responses
module.exports.SUCCESS = "SUCCESS";
module.exports.DATA = "DATA";
module.exports.QUERY_FINISH = "FINISHED";
module.exports.ERROR = "ERROR";
module.exports.EXECUTING = "EXECUTING";

//For Join
module.exports.SEC_DATA= "SEC_DATA";
module.exports.PAYLOAD_TYPE_DATA_SEARCH= "DATASEARCH";

//For executors 
module.exports.STORE = 0;
module.exports.DISCARD = 1;

//For messages
module.exports.CONFIG_MESSAGE = "CONFIG";
module.exports.DATA_MESSAGE = "DATA";
module.exports.SIMPLE_TYPE_MESSAGE = "simple";
module.exports.JOIN_TYPE_MESSAGE = "join";

// PHT
module.exports.PASS_NO_INDEX_TAG = "PASS_NO_IND";