var Q 					= require('q');
var util 				= require('../src/helpers');
var Connection 	= require('../src/connection');

util.inherits(MockConnection, Connection);

module.exports = MockConnection;

function MockConnection () {
	this.adapter = {
		executeQuery: function () {
			return Q.Promise(function (resolve, reject) {
				process.nextTick(function () {
					resolve();
				});
			});
		}
	};
}