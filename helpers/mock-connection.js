var _						= require('lodash');
var Q 					= require('q');
var util 				= require('../src/helpers');

var first				= _.first;
var extend			= _.extend;
var isDefined		= function (value) {
	return !(_.isUndefined(value));
};
var isUndefined	= _.isUndefined;

var assert			= require('assert');
var Connection 	= require('../src/connection');

util.inherits(MockConnection, Connection);

module.exports = MockConnection;

function MockConnection () {
	var _this = this;

	var queryExpectationList = this.queryExpectationList = [];

	this.responses = [];
	this.adapter = {
		executeQuery: function (query) {
			var response = {
			};

			if(queryExpectationList.length > 0) {
				assert.equal(first(queryExpectationList), query, 'unexpect database query');
				
				if(first(queryExpectationList) == query) {
					queryExpectationList.splice(queryExpectationList.indexOf(query), 1);
				}

				if(isDefined(_this.responses[query])) {
					response.rows = _this.responses[query];
				}
			}

			return Q.Promise(function (resolve, reject) {
				process.nextTick(function () {
					resolve(response);
				});
			});
		}
	};
}

extend(MockConnection.prototype, {
	expectQuery: function (query) {
		var _this = this;

		this.queryExpectationList.push(query);

		return {
			respond: function (response) {
				_this.responses[query] = response;
			}
		};
	}
});