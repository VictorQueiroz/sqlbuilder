var Q 					= require('q');
var util 				= require('../src/helpers');
var Connection 	= require('../src/connection');

util.inherits(MockConnection, Connection);

module.exports = MockConnection;

function MockConnection () {
	var queryExpectationList = this.queryExpectationList = [];
	this.adapter = {
		executeQuery: function (query) {
			if(queryExpectationList.length > 0) {
				assert.equal(first(queryExpectationList), query, 'unexpect database query');
				
				if(first(queryExpectationList) == query) {
					queryExpectationList.splice(queryExpectationList.indexOf(query), 1);
				}
			}

			return Q.Promise(function (resolve, reject) {
				process.nextTick(function () {
					resolve();
				});
			});
		}
	};
}

extend(MockConnection.prototype, {
	expectQuery: function (query) {
		this.queryExpectationList.push(query);
	}
});