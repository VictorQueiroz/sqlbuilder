var _ = require('lodash');
var util = require('../helpers');
var inflection = require('inflection');

var trim = String.prototype.trim.apply,
		keys = _.keys,
		first = _.first,
		filter = _.filter,
		values = _.values,
		isArray = _.isArray,
		isEmpty = _.isEmpty,
		forEach = _.forEach;

module.exports = Grammar;

var BaseGrammar = require('./base-grammar');

function Grammar() {
	BaseGrammar.call(this);

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

util.inherits(Grammar, BaseGrammar);

_.extend(Grammar.prototype, {
	compileSelect: function (query) {
		if(isEmpty(query.columns)) {
			query.columns = ['*'];
		}

		return this.concatenate(this.compileComponents(query)).trim();
	},

	compileComponents: function (query) {
		var sql = [];

		forEach(this.selectComponents, function (component) {
			var value = query[component];

			if(!isEmpty(value)) {
				var method = 'compile' + inflection.capitalize(component);
				sql[component] = this[method](query, value);
			}
		}, this);

		return sql;
	},
	compileAggregate: function (query, aggregate) {
		var column = this.columnize(aggregate.columns);

		// If the query has a "distinct" constraint and we're not asking for all columns
    // we need to prepend "distinct" onto the column name so that the query takes
    // it into account when it performs the aggregating operations on the data.
    if(query.isDistinct() && column !== '*') {
    	column = `distinct ${column}`;
    }

    return `select ${aggregate.function}(${column}) as aggregate`;
	},

	compileColumns: function (query, columns) {
		// If the query is actually performing an aggregating select, we will let that
    // compiler handle the building of the select clauses, as it will need some
    // more syntax that is best handled by that function to keep things neat.
    if(!isEmpty(query.aggregate)) {
    	return;
    }

    var select = query.isDistinct() ? 'select distinct ' : 'select ';

    return `${select}${this.columnize(columns)}`;
	},

	compileFrom: function (query, table) {
		return `from ${this.wrapTable(table)}`;
	},

	compileJoins: function (query, joins) {
		var sql = [];

		query.setBindings([], 'join');

		forEach(joins, function (join) {
			var table = this.wrapTable(join.table);

			// First we need to build all of the "on" clauses for the join. There may be many
	    // of these clauses so we will need to iterate through each one and build them
	    // separately, then we'll join them up into a single string when we're done.
	    var clauses = [];

	    forEach(join.clauses, function(clause) {
	    	clauses.push(this.compileJoinConstraint(clause));
	    }, this);

	    forEach(join.bindings, function (binding) {
	    	query.addBinding(binding, 'join');
	    });

	    // Once we have constructed the clauses, we'll need to take the boolean connector
      // off of the first clause as it obviously will not be required on that clause
      // because it leads the rest of the clauses, thus not requiring any boolean.
      clauses[0] = this.removeLeadingBoolean(clauses[0]);

      clauses = clauses.join(' ');

      var type = join.type;

      // Once we have everything ready to go, we will just concatenate all the parts to
      // build the final join statement SQL for the query and we can then return the
      // final clause back to the callers as a single, stringified join statement.
      sql.push([type, 'join', table, 'on', clauses].join(' '));
		}, this);

		return sql.join(' ');
	},

	compileJoinConstraint: function (clause) {
		var first = this.wrap(clause.first);

		var second = '';

		if(clause.where) {
			if(clause.operator === 'in' || clause.operator === 'not in') {
				second += `(${fill([], '?', 0, clause['second']).join(', ')})`;
			} else {
				second += '?';
			}
		} else {
			second += this.wrap(clause.second);
		}

		return `${clause.boolean} ${first} ${clause.operator} ${second}`;
	},

	compileWheres: function (query) {
		var sql = [];

		if(isEmpty(query.wheres)) {
			return '';
		}

		// Each type of where clauses has its own compiler function which is responsible
    // for actually creating the where clauses SQL. This helps keep the code nice
    // and maintainable since each clause has a very small method that it uses.
    forEach(query.wheres, function (where) {
    	var method = 'where' + where['type'];

    	sql.push(where['boolean'] + ' ' + this[method](query, where));
    }, this);

    // If we actually have some where clauses, we will strip off the first boolean
    // operator, which is added by the query builders for convenience so we can
    // avoid checking for the first clauses in each of the compilers methods.
    if(sql.length > 0) {
    	sql = sql.join(' ');

    	return 'where ' + this.removeLeadingBoolean(sql);
    }

    return '';
	},

	whereNested: function (query, where) {
		var nested = where.query;

		return '(' + this.compileWheres(nested).substr(6) + ')';
	},

	whereSub: function (query, where) {
		var select = this.compileSelect(where.query);

		return [this.wrap(where.column), where.operator, '(' + select + ')'].join(' ');
	},

	whereBasic: function (query, where) {
		var value = this.parameter(where.value);

		return [this.wrap(where.column), where.operator, value].join(' ');
	},

	whereBetween: function (query, where) {
		var between = where.hasOwnProperty('not') ? 'not between' : 'between';

		return [this.wrap(where.column), between, '? and ?'].join(' ');
	},

	whereExists: function (query, where) {
		return 'exists (' + this.compileSelect(where.query) + ')';
	},

	whereNotExists: function (query, where) {
		return 'not exists (' + this.compileSelect(where.query) + ')';
	},

	whereIn: function (query, where) {
		if(isEmpty(where.values)) {
			return '0 = 1';
		}

		var values = this.parameterize(where.values);

		return this.wrap(where.column) + ' in (' + values + ')';
	},

	whereNotIn: function (query, where) {
		if(isEmpty(where.values)) {
			return '1 = 1';
		}

		var values = this.parameterize(where.values);

		return this.wrap(where.column) + ' not in (' + values + ')';
	},

	whereInSub: function (query, where) {
		var select = this.compileSelect(where.query);

		return this.wrap(where.column) + ' in (' + select + ')';
	},

	whereNotInSub: function (query, where) {
		var select = this.compileSelect(where.query);

		return this.wrap(where.column) + ' not in (' + select + ')';
	},

	whereNull: function (query, where) {
		return this.wrap(where.column) + ' is null';
	},

	whereNotNull: function (query, where) {
		return this.wrap(where.column) + ' is not null';
	},

	whereDate: function (query, where) {
		return this.dateBasedWhere('date', query, where);
	},

	whereDay: function (query, where) {
		return this.dateBasedWhere('day', query, where);
	},

	whereMonth: function (query, where) {
		return this.dateBasedWhere('month', query, where);
	},

	whereYear: function (query, where) {
		return this.dateBasedWhere('year', query, where);
	},

	dateBasedWhere: function (type, query, where) {
		var value = this.parameter(where.value);

		return type + '(' + this.wrap(where.column) + ') ' + where.operator + ' ' + value;
	},

	whereRaw: function (query, where) {
		return where.sql;
	},

	compileGroups: function (query, havings) {
		var sql = map([this, 'compileHaving'], havings).join(' ');
		return 'having ' + this.removeLeadingBoolean(sql);
	},

	compileHaving: function (having) {
		// If the having clause is "raw", we can just return the clause straight away
    // without doing any more processing on it. Otherwise, we will compile the
    // clause into SQL based on the components that make it up from builder.
    if(having.type === 'raw') {
    	return having.boolean + ' ' + having.sql;
    }

    return this.compileBasicHaving(having);
	},

	compileBasicHaving: function (having) {
		var column = this.wrap(having.column);
		var parameter = this.parameter(having.value);

		return having.boolean + ' ' + column + ' ' + having.operator + ' ' + parameter;
	},

	compileOrders: function (query, orders) {
		return 'order by ' + map(orders, function (order) {
			if(order.hasOwnProperty('sql')) {
				return order.sql;
			}

			return this.wrap(order.column) + ' ' + order.direction;
		}).join(', ');
	},

	compileLimit: function (query, limit) {
		return 'limit ' + limit;
	},

	compileOffset: function (query, offset) {
		return 'offset' + offset;
	},

	compileUnions: function (query) {
		var sql = '';

		forEach(query.unions, function (union) {
			sql += this.compileUnion(union);
		}, this);

		if(query.hasOwnProperty('unionOrders')) {
			sql += ' ' + this.compileOrders(query, query.unionOrders);
		}

		if(query.hasOwnProperty('unionLimit')) {
			sql += ' ' + this.compileLimit(query, query.unionLimit);
		}

		if(query.hasOwnProperty('unionOffset')) {
			sql += ' ' + this.compileOffset(query, query.unionOffset);
		}

		return trim(sql);
	},

	compileUnion: function (union) {
		var joiner = union.all ? ' union all' : ' union ';
		return joiner[union.query].toSql();
	},

	compileInsert: function (query, values) {
		// Essentially we will force every insert to be treated as a batch insert which
    // simply makes creating the SQL easier for us since we can utilize the same
    // basic routine regardless of an amount of records given to us to insert.
    var table = this.wrapTable(query.from);

    if(!isArray(first(values))) {
    	values = [values];
    }

    var columns = this.columnize(keys(first(values)));

    // We need to build a list of parameter place-holders of values that are bound
    // to the query. Each insert should have the exact same amount of parameter
    // bindings so we will loop through the record and parameterize them all.
    var parameters = [];

    forEach(values, function (record) {
    	parameters.push(`(${this.parameterize(record)})`);
    }, this);

    parameters = parameters.join(', ');

    return `insert into ${table} (${columns}) values ${parameters}`;
	},

	compileInsertGetId: function (query, values, sequence) {
		return this.compileInsert(query, values);
	},

	compileUpdate: function (query, values) {
		var table = this.wrapTable(query.from);

		// Each one of the columns in the update statements needs to be wrapped in the
    // keyword identifiers, also a place-holder needs to be created for each of
    // the values in the list of bindings so we can make the sets statements.
    var columns = [];

    forEach(values, function (value, key) {
    	columns.push(this.wrap(key) + ' = ' + this.parameter(value));
    }, this);

    columns = columns.join(', ');

    var joins = '';

    // If the query has any "join" clauses, we will setup the joins on the builder
    // and compile them so we can attach them to this update, as update queries
    // can get join statements to attach to other tables when they're needed.
    if(query.hasOwnProperty('joins')) {
    	joins += ' ' + this.compileJoins(query, query.joins);
    }

    // Of course, update queries may also be constrained by where clauses so we'll
    // need to compile the where clauses and attach it to the query so only the
    // intended records are updated by the SQL statements we generate to run.
    var where = this.compileWheres(query);

    return trim('update ' + table + joins + ' set ' + columns + where);
	},

	compileDelete: function (query) {
		var table = this.wrapTable(query.from);

		var where = isArray(query.wheres) ? this.compileWheres(query) : '';

		return trim('delete from ' + table + ' ' + where);
	},

	compileTruncate: function (query) {
		var obj = {};

		obj[this.wrapTable(query.from)] = {};

		return obj;
	},

	compileLock: function (query, value) {
		return isString(value) ? value : '';
	},

	concatenate: function (segments) {
		return filter(values(segments), function (segment) {
			return !isEmpty(segment);
		}).join(' ');
	},

	removeLeadingBoolean: function (value) {
		return value.replace(/and |or /g, '');
	}
});