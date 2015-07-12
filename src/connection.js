var _ 			= require('lodash'),
		extend	= _.extend,
		forEach = _.forEach;

module.exports = Connection;

var adapters = {
	mysql: require('./adapters/mysql-adapter')
};

function Connection(options) {
	options = options || {};
	options.adapter = options.adapter || 'mysql';

	var Adapter = adapters[options.adapter];

	this.adapter = new Adapter(options);

	if(!this.adapter) {
		throw new Error('You must specify a valid sql adapter');
	}
}

extend(Connection.prototype, {
	executeQuery: function (rawText) {
		return this.adapter.executeQuery(rawText);
	},

	resolveBindings: function (sql, bindings) {
		forEach(bindings, function (binding) {
			sql = sql.replace('?', `"${binding}"`);
		});

		return sql;
	},

	insert: function (sql, bindings) {
		var rawText = this.resolveBindings(sql, bindings);

		return this.executeQuery(rawText);
	},

	select: function () {
		return this.insert.apply(this, arguments);
	}
});