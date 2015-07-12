var Q 			  = require('q');
var _					= require('lodash');
var util 			= require('../helpers');
var mysql 			= require('mysql');
var extend		= _.extend;
var Adapter 	= require('./adapter');

module.exports = MySqlAdapter;

function MySqlAdapter(options) {
	Adapter.call(this);

	this.connection = mysql.createConnection(options);
	this.connection.connect();
}

util.inherits(MySqlAdapter, Adapter);

extend(MySqlAdapter.prototype, {
	executeQuery: function (rawText) {
		var deferred = Q.defer();

		this.connection.query(rawText, function (err, rows, fields) {
			if(err) {
				return deferred.reject(err);
			}

			deferred.resolve({
				rows: rows,
				fields: fields
			});
		});

		return deferred.promise;
	},
	destroy: function () {
		return this.connection.end();
	}
});