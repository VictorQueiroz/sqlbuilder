var _ = require('lodash');
var util = require('../helpers');
var Grammar = require('./grammar');

module.exports = MySqlGrammar;

util.inherits(MySqlGrammar, Grammar);

function MySqlGrammar () {
	Grammar.call(this);

	this.selectComponents = [
	  'aggregate',
	  'columns',
	  'from',
	  'joins',
	  'wheres',
	  'groups',
	  'havings',
	  'orders',
	  'limit',
	  'offset',
	  'unions',
	  'lock'
	];
}

_.extend(MySqlGrammar.prototype, {
	compileSelect: function (query) {
		var sql = this.parent.compileSelect.apply(this, arguments);

		if(query.unions) {
			sql = `(${sql}) ${this.compileUnions(query)}`;
		}

		return sql;
	},

	compileUnion: function (union) {
		var joiner = union.all ? ' union all ' : ' union ';

		return `${joiner}(${union.query.toSql()})`;
	},


  /**
   * Compile the lock into SQL.
   */
	compileLock: function (query, value) {
		if(isString(value)) {
			return value;
		}

		return value ? 'for update' : 'lock in share mode';
	},

	/**
   * Compile an update statement into SQL.
   */
  compileUpdate: function (query, values) {
  	var sql = this.parent.compileUpdate.apply(this, arguments);

  	if(query.orders) {
  		sql += ` ${this.compileOrders(query, query.orders)}`;
  	}

  	if(query.limit) {
  		sql += ` ${this.compileLimit(query, query.limit)}`;
  	}

  	return util.rtrim(sql);
  },

  compileDelete: function (query) {
  	var table = this.wrapTable(query.from);
  	var where = isArray(query.wheres) ? this.compileWheres(query) : '';

  	var sql;

  	if(query.joins) {
  		var joins = ` ${this.compileJoins(query, query.joins)}`;

  		sql = `delete ${table} from ${table}${joins} ${where}`.trim();
  	} else {
  		sql = `delete from ${table} ${where}`;
  	}

  	if(query.orders) {
  		sql += ` ${this.compileOrders(query, query.orders)}`;
  	}

  	if(query.limit) {
  		sql += ` ${this.compileLimit(query, query.limit)}`;
  	}

  	return sql;
  },

  /**
   * Wrap a single string in keyword identifiers.
   */
  wrapValue: function (value) {
  	if(value === '*') {
  		return value;
  	}

  	return `\`${value.replace('\`', '\`\`')}\``;
  }
});