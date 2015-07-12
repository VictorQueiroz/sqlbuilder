var _ = require('lodash'),
		isUndefined = _.isUndefined;

var Expression = require('./expression');

module.exports = JoinClause;

function JoinClause (type, table) {
	this.type = type;
	this.table = table;
	this.clauses = [];
	this.bindings = [];
}

JoinClause.prototype = {
	/**
   * Add an "on" clause to the join.
   */
	on: function (first, operator, second, boolean, where) {
		boolean = boolean || 'and';

		if(where) {
			this.bindings.push(second);
		}

		if(where && (operator === 'in' || operator === 'not in') && isArray(second)) {
			second = second.length;
		}

		this.clauses.push({
			first: first,
			operator: operator,
			second: second,
			boolean: boolean,
			where: where
		});

		return this;
	},

	/**
	 * Add an "or on" clause to the join.
	 */
	orOn: function (first, operator, second) {
		return this.on(first, operator, second, 'or');
	},

	where: function (first, operator, second, boolean) {
		boolean = boolean || 'and';

		return this.on(first, operator, second, boolean, true);
	},

	/**
	 * Add an "or on where" clause to the join.
	 */
	orWhere: function (first, operator, second) {
		return this.on(first, operator, second, 'or', true);
	},

	/**
	 * Add an "on where is null" clause to the join.
	 */
	whereNull: function (column, boolean) {
		boolean = boolean || 'and';

		return this.on(column, 'is', new Expression('null'), boolean, false);
	},

	orWhereNull: function (column) {
		return this.whereNull(column, 'or');
	},

	whereNotNull: function (column, boolean) {
		return this.on(column, 'is', new Expression('not null'), boolean, false);
	},

	orWhereNotNull: function (column) {
		return this.whereNotNull(column, 'or');
	},

	whereIn: function (column, values) {
		return this.on(column, 'in', values, 'and', true);
	},

	/**
   * Add an "on where not in (...)" clause to the join.
   */
  whereNotIn: function (column, values) {
  	return this.on(column, 'not in', values, 'and', true);
  },

  orWhereIn: function (column, values) {
  	return this.on(column, 'in', values, 'or', true);
  },

  orWhereNotIn: function (column, values) {
  	return this.on(column, 'not in', values, 'or', true);
  }
};