var _ 			= require('lodash'),
		extend	= _.extend;

var Connection = require('./src/connection');
var QueryBuilder = require('./src/querybuilder');

var grammars = {
	mysql: require('./src/grammars/mysql-grammar')
};

module.exports = function (options) {
	var connection = new Connection(options);
	var Grammar = grammars[options.adapter];

	if(!Grammar) {
		throw new Error('You must specify a valid sql adapter');
	}

	return new QueryBuilder(connection, new Grammar());
};

extend(module.exports, {
	QueryBuilder: QueryBuilder
});