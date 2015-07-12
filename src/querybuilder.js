var _ = require('lodash'),
		map = _.map,
		last = _.last,
		first = _.first,
		filter = _.filter,
		values = _.values,
		extend = _.extend,
		isEmpty = _.isEmpty,
		isArray = _.isArray,
		toArray = _.toArray,
		forEach = _.forEach,
		isFunction = _.isFunction,
		flattenDeep = _.flattenDeep;

function Collection(items) {
	this.items = items;
}

var JoinClause = require('./joinclause');
var Expression = require('./expression');

module.exports = Builder;

function Builder (connection, grammar) {
	this.grammar = grammar;
	this.connection = connection;

	this.bindings = {
		select: [],
		join: [],
		where: [],
		having: [],
		order: []
	};

	this._distinct = false;

	this.joins = [];
	this.wheres = [];
	this.groups = [];
	this.backups = [];
	this.operators = [
		'=', '<', '>', '<=', '>=', '<>', '!=',
		'like', 'like binary', 'not like', 'between', 'ilike',
		'&', '|', '^', '<<', '>>',
		'rlike', 'regexp', 'not regexp',
		'~', '~*', '!~', '!~*', 'similar to',
		'not similar to',
	];
}

Builder.prototype = {
	select: function (columns) {
		this.columns = isArray(columns) ? columns : toArray(arguments);

		return this;
	},

	selectRaw: function (expression, bindings) {
		this.addSelect(new Expression(expression));

		if(bindings) {
			this.addBinding(bindings, 'select');
		}
	},

	/**
	 * Add a subselect expression to the query.
	 */
	selectSub: function (query, as) {
		var cb,
				bindings = [];

		if(isFunction(query)) {
			cb(query = this.newQuery());
		}

		if(query instanceof Builder) {
			extend(bindings, query.getBindings());

			query = query.toSql();
		} else if(isString(query)) {
			this.bindings = [];
		} else {
			throw new Error('Invalid argument');
		}

		this.selectRaw('(' + query + ') as ' + this.grammar.wrap(as), bindings);
	},

	/**
	 * Add a new select column to the query.
	 */
	addSelect: function (column) {
		column = isArray(column) ? column : toArray(arguments);

		this.columns = this.columns.concat(column);

		return this;
	},

	isDistinct: function () {
		return this._distinct;
	},

	/**
	 * Force the query to only return distinct results.
	 */
	distinct: function () {
		this._distinct = true;

		return this;
	},

	/**
	 * Set the table which the query is targeting.
	 */
	from: function (table) {
		this.from = table;

		return this;
	},

	/**
	 * Add a join clause to the query.
	 */
	join: function (table, one, operator, two, type, where) {
		type = type || 'inner';
		where = where || false;

		// If the first "column" of the join is really a Closure instance the developer
		// is trying to build a join with a complex "on" clause containing more than
		// one condition, so we'll add the join and call a Closure with the query
		if(isFunction(one)) {
			this.joins.push(new JoinClause(type, table));

			one(last(this.joins));
		}

		// If the column is simply a string, we can assume the join simply has a basic
		// "on" clause with a single condition. So we will just build the join with
		// this simple join clauses attached to it. There is not a join callback.
		else {
			var join = new JoinClause(type, table);

			this.joins.push(
				join.on(one, operator, two, 'and', where)
			);
		}

		return this;
	},

	/**
	 * Add a "join where" clause to the query.
	 */
	joinWhere: function (table, one, operator, two, type) {
		type = type || 'inner';

		return this.join(table, one, operator, two, type, true);
	},

	leftJoin: function (table, first, operator, second) {
		return this.join(table, first, operator, second, 'left');
	},

	leftJoinWhere: function (table, one, operator, two) {
		return this.joinWhere(table, one, operator, two, 'left');
	},

	rightJoin: function (table, first, operator, second) {
		return this.join(table, first, operator, second, 'right');
	},

	rightJoinWhere: function (table, one, operator, two) {
		return this.joinWhere(table, one, operator, two, 'right');
	},

	where: function (column, operator, value, boolean) {
		boolean = boolean || 'and';

		// If the column is an array, we will assume it is an array of key-value pairs
		// and can add them each as a where clause. We will maintain the boolean we
		// received when the method was called and pass it into the nested where.
		if(isArray(column)) {
			return this.whereNested(function (query) {
				forEach(column, function (value, key) {
					query.where(key, '=', value);
				});
			}, boolean);
		}

		// Here we will make some assumptions about the operator. If only 2 values are
		// passed to the method, we will assume that the operator is an equals sign
		// and keep going. Otherwise, we'll require the operator to be passed in.
		if(arguments.length == 2) {
			value = operator;
			operator = '=';
		} else if (this.invalidOperatorAndValue(operator, value)) {
			throw new Error('Illegal operator and value combination.');
		}

		// If the columns is actually a Closure instance, we will assume the developer
		// wants to begin a nested where statement which is wrapped in parenthesis.
		// We'll add that Closure to the query then return back out immediately.
		if(isFunction(column)) {
			return this.whereNested(column, boolean);
		}

		// If the given operator is not found in the list of valid operators we will
		// assume that the developer is just short-cutting the '=' operators and
		// we will set the operators to '=' and set the values appropriately.
		if(this.operators.indexOf(operator.toLowerCase()) === -1) {
			value = operator;
			operator = value;
		}

		// If the value is a Closure, it means the developer is performing an entire
		// sub-select within the query and we will need to compile the sub-select
		// within the where clause to get the appropriate query record results.
		if(isFunction(value)) {
			return this.whereSub(column, operator, value, boolean);
		}

		// If the value is "null", we will just assume the developer wants to add a
		// where null clause to the query. So, we will allow a short-cut here to
		// that method for convenience so the developer doesn't have to check.
		if(value === 'null') {
			return this.whereNull(column, boolean, operator != '=');
		}

		// Now that we are working with just a simple query we can put the elements
		// in our array and add the query binding to our array of bindings that
		// will be bound to each SQL statements when it is finally executed.
		type = 'Basic';

		this.wheres.push({
			type: type,
			column: column,
			operator: operator,
			value: value,
			boolean: boolean
		});

		if(!(value instanceof Expression)) {
			this.addBinding(value, 'where');
		}

		return this;
	},

	orWhere: function (column, operator, value) {
		return this.where(column, operator, value, 'or');
	},

	invalidOperatorAndValue: function (operator, value) {
		var isOperator = this.operators.indexOf(operator.toLowerCase()) > -1;

		return isOperator && operator != '=' && value === 'null';
	},

	whereRaw: function (sql, bindings, boolean) {
		var type = 'raw';

		boolean = boolean || 'and';

		this.wheres.push({
			type: type,
			sql: sql,
			boolean: boolean
		});

		this.addBinding(bindings, 'where');
	},

	orWhereRaw: function (sql, bindings) {
		return this.whereRaw(sql, bindings, 'or');
	},

	whereBetween: function (column, 	values, boolean, not) {
		var type = 'between';

		boolean = boolean || 'and';
		not = not || false;

		this.wheres.push({
			column: column,
			type: type,
			boolean: boolean,
			not: not
		});

		this.addBinding(values, 'where');
	},

	orWhereBetween: function (column, values) {
		return this.whereBetween(column, values, 'or');
	},

	whereNotBetween: function (column, values, boolean) {
		boolean = boolean || 'and';

		return this.whereBetween(column, values, boolean, true);
	},

	orWhereNotBetween: function (column, values) {
		return this.whereNotBetween(column, values, 'or');
	},

	whereNested: function (callback, boolean) {
		// To handle nested queries we'll actually create a brand new query instance
		// and pass it off to the Closure that we have. The Closure can simply do
		// do whatever it wants to a query then we will store it for compiling.
		var query = this.newQuery();

		query.from(this.from);

		callback(query);

		return this.addNestedWhereQuery(query, boolean);
	},

	addNestedWhereQuery: function (query, boolean) {
		boolean = boolean || 'and';

		if(query.wheres.length) {
			var type = 'nested';

			this.wheres.push({
				type: type,
				query: query,
				boolean: boolean
			});

			this.mergeBindings(query);
		}
	},

	whereSub: function (column, operator, callback, boolean) {
		var type = 'sub';
		var query = this.newQuery();

		// Once we have the query instance we can simply execute it so it can add all
		// of the sub-select's conditions to itself, and then we can cache it off
		// in the array of where clauses for the "main" parent query instance.
		callback(query);

		this.wheres.push({
			type: type,
			column: column,
			operator: operator,
			query: query,
			boolean: boolean
		});

		this.mergeBindings(query);
	},

	/**
	 * Add an exists clause to the query.
	 */
	whereExists: function (callback, boolean, not) {
		boolean = boolean || 'and';
		not = not || false;

		var type = not ? 'NotExists' : 'Exists';
		var query = this.newQuery();

		// Similar to the sub-select clause, we will create a new query instance so
		// the developer may cleanly specify the entire exists query and we will
		// compile the whole thing in the grammar and insert it into the SQL.
		callback(query);

		this.wheres.push({
			type: type,
			operator: operator,
			query: query,
			boolean: boolean
		});

		this.mergeBindings(query);
	},

	/**
	 * Add an or exists clause to the query.
	 */
	orWhereExists: function (callback, not) {
		return this.whereExists(callback, 'or', not);
	},

	whereNotExists: function (callback, boolean) {
		boolean = boolean || 'and';

		return this.whereExists(callback, boolean, true);
	},

	orWhereNotExists: function (callback) {
		return this.orWhereExists(callback, true);
	},

	whereIn: function (column, values, boolean, not) {
		var type = not ? 'NotIn' : 'In';

		boolean = boolean || 'and';

		// If the value of the where in clause is actually a Closure, we will assume that
		// the developer is using a full sub-select for this "in" statement, and will
		// execute those Closures, then we can re-construct the entire sub-selects.
		if(isFunction(values)) {
			return this.whereInSub(column, values, boolean, not);
		}

		this.wheres.push({
			type: type,
			column: column,
			values: values,
			boolean: boolean
		});

		this.addBinding(values, 'where');
	},

	orWhereIn: function (column, values) {
		return this.whereIn(column, values, 'or');
	},

	whereNotIn: function (column, values, boolean) {
		boolean = boolean || 'and';

		return this.whereIn(column, values, boolean, true);
	},

	orWhereNotIn: function (column, values) {
		return this.whereNotIn(column, values, 'or');
	},

	whereInSub: function (column, callback, boolean, not) {
		var type = not ? 'NotInSub' : 'InSub';
		var query = this.newQuery();

		// To create the exists sub-select, we will actually create a query and call the
		// provided callback with the query so the developer may set any of the query
		// conditions they want for the in clause, then we'll put it in this array.
		callback(query);

		this.wheres.push({
			type: type,
			column: column,
			query: query,
			boolean: boolean
		});

		this.mergeBindings(query);
	},

	whereNull: function (column, boolean, not) {
		var type = not ? 'NotNull' : 'Null';

		boolean = boolean || 'and';

		this.wheres.push({
			type: type,
			column: column,
			boolean: boolean
		});
	},

	orWhereNull: function (column) {
		return this.whereNull(column, 'or');
	},

	whereNotNull: function (column, boolean) {
		boolean = boolean || 'and';

		return this.whereNull(column, boolean, true);
	},

	orWhereNotNull: function (column) {
		return this.whereNotNull(column, 'or');
	},

	whereDate: function (column, operator, value, boolean) {
		boolean = boolean || 'and';

		return this.addDateBasedWhere('Date', column, operator, value, boolean);
	},

	whereDay: function (column, operator, value, boolean) {
		boolean = boolean || 'and';

		return this.addDateBasedWhere('Day', column, operator, value, boolean);
	},

	whereMonth: function (column, operator, value, boolean) {
		boolean = boolean || 'and';

		return this.addDateBasedWhere('Month', column, operator, value, boolean);
	},

	whereYear: function whereYear(column, operator, value, boolean) {
		boolean = boolean || 'and';

		return this.addDateBasedWhere('Year', column, operator, value, boolean);
	},

	addDateBasedWhere: function (type, column, operator, value, boolean) {
		boolean = boolean || 'and';

		this.wheres.push({
			column: column,
			type: type,
			boolean: boolean,
			operator: operator,
			value: value
		});

		this.addBinding(value, 'where');
	},

	dynamicWhere: function (method, parameters) {
		var finder = method.substr(5);
		var segments = finder.split(/(And|Or)(?=[A-Z])/);

		// The connector variable will determine which connector will be used for the
		// query condition. We will change it as we come across new boolean values
		// in the dynamic method strings, which could contain a number of these.
		var connector = 'and';

		var index = 0;

		forEach(segments, function (segment) {
			// If the segment is not a boolean connector, we can assume it is a column's name
			// and we will add it to the query as a new constraint as a where clause, then
			// we can keep iterating through the dynamic method string's segments again.
			if(segment != 'And' && segment != 'Or') {
				this.addDynamic(segment, connector, parameters, index);

				index++;
			}

			// Otherwise, we will store the connector so we know how the next where clause we
			// find in the query should be connected to the previous ones, meaning we will
			// have the proper boolean connector to connect the next where clause found.
			else {
				connector = segment;
			}
		}, this);
	},

	addDynamic: function (segment, connector, parameters, index) {
		// Once we have parsed out the columns and formatted the boolean operators we
		// are ready to add it to this query as a where clause just like any other
		// clause on the query. Then we'll increment the parameter index values
		var bool = connector.toLowerCase();

		this.where(segment, '=', parameters[index], bool);
	},

	groupBy: function () {
		var args = _.toArray(arguments);

		forEach(args, function (arg) {
			this.groups = this.groups.concat(isArray(arg) ? arg : [arg]);
		}, this);
	},

	/**
	 * Add a "having" clause to the query.
	 */
	having: function (column, operator, value, boolean) {
		var type = 'basic';

		this.havings.push({
			type: type,
			column: column,
			operator: operator,
			value: value,
			boolean: boolean
		});

		if(!value instanceof Expression) {
			this.addBinding(value, 'having');
		}
	},

	orHaving: function (column, operator, value) {
		return this.having(column, operator, value, 'or');
	},

	havingRaw: function (sql, bindings, boolean) {
		var type = 'raw';

		bindings = bindings || [];
		boolean = boolean || 'and';

		this.addBinding(bindings, 'having');
	},

	orHavingRaw: function (sql, bindings) {
		bindings = bindings || [];

		return this.havingRaw(sql, bindings, 'or');
	},

	orderBy: function (column, direction) {
		var property = this.unions ? 'unionOrders' : 'orders';
		var direction = direction.toLowerCase() == 'asc' ? 'asc' : 'desc';

		this[property].push({
			column: column,
			direction: direction
		});
	},

	latest: function (column) {
		column = column || 'created_at';

		return this.orderBy(column, 'desc');
	},

	oldest: function (column) {
		column = column || 'created_at';

		return this.orderBy(column, 'asc');
	},

	orderByRaw: function (sql, bindings) {
		var type = 'raw';
		var property = this.unions ? 'unionOrders' : 'orders';

		bindings = bindings || [];

		this[property].push({
			type: type,
			sql: sql
		});

		this.addBinding(bindings, 'order');
	},

	offset: function (value) {
		var property = this.unions ? 'unionOffset' : 'offset';

		this[`_${property}`] = Math.max(0, value);
	},

	skip: function (value) {
		return this.offset(value);
	},

	limit: function (value) {
		var property = this.unions ? 'unionLimit': 'limit';

		if(value > 0) {
			this[`_${property}`] = value;
		}

		return this;
	},

	take: function (value) {
		return this.limit(value);
	},

	forPage: function (page, perPage) {
		return this.skip((page - 1) * perPage).take(perPage);
	},

	union: function (query, all) {
		if(isFunction (query)) {
			query(query = this.newQuery());
		}

		this.unions.push({
			query: query,
			all: all
		});

		return this.mergeBindings(query);
	},

	unionAll: function (query) {
		return this.union(query, true);
	},

	lock: function (value) {
		this.lock = value;
	},

	lockForUpdate: function () {
		return this.lock(true);
	},

	sharedLock: function () {
		return this.lock(false);
	},

	toSql: function () {
		return this.grammar.compileSelect(this);
	},

	find: function (id, columns) {
		columns = columns || ['*'];

		return this.where('id', '=', id).first(columns);
	},

	value: function (column) {
		var result = this.first([column]);

		return result.length > 0 ? first(result) : null;
	},

	pluck: function (column) {
		return this.value(column);
	},

	first: function (columns) {
		columns = columns || ['*'];

		var results = this.take(1).get(columns);

		return results.length > 0 ? first(results) : null;
	},

	/**
	 * Execute the query as a "select" statement.
	 */
	get: function (columns) {
		columns = columns || ['*'];

		return this.getFresh();
	},

	getFresh: function (columns) {
		columns = columns || ['*'];

		if(isEmpty(this.columns)) {
			this.columns = columns;
		}

		return this.runSelect();
	},

	runSelect: function () {
		return this.connection.select(this.toSql(), this.getBindings());
	},

	backupFieldsForCount: function () {
		forEach(['orders', 'limit', 'offset', 'columns'], function (field) {
			this.backups[field] = this[field];
			this.field = null;
		}, this);
	},

	restoreFieldsForCount: function () {
		forEach(['orders', 'limit', 'offset', 'columns'], function (field) {
			this[field] = this.backups[field];
		});

		this.backups = [];
	},

	lists: function (column, key) {
		key = key || null;

		var columns = this.getListSelect(column, key);
		var results = new Collection(this.get(columns));

		return results.pluck(columns[0], columns[1]).all();
	},

	getListSelect: function (column, key) {
		var select = isEmpty(key) ? [column] : [column, key];

		// If the selected column contains a "dot", we will remove it so that the list
		// operation can run normally. Specifying the table is not needed, since we
		// really want the names of the columns as it is in this resulting array.

		return map(select, function (column) {
			var dot = column.indexOf('.');

			return dot === -1 ? column : column.substr(dot + 1);
		});
	},

	exists: function () {
		var limit = this.limit;
		var result = this.limit(1).count() > 0;

		this.limit(limit);

		return result;
	},

	insert: function (values) {
		if(isEmpty(values)) {
			return true;
		}

		// We'll treat every insert like a batch insert so we can easily insert each
		// of the records into the database consistently. This will make it much
		// easier on the grammars to just handle one type of record insertion.
		var bindings = [];

		forEach(values, function (record) {
			forEach(record, function (value) {
				bindings.push(value);
			});
		});

		var sql = this.grammar.compileInsert(this, values);

		// Once we have compiled the insert statement's SQL we can execute it on the
		// connection and return a result as a boolean success indicator as that
		// is the same type of result returned by the raw connection instance.
		bindings = this.cleanBindings(bindings);

		return this.connection.insert(sql, bindings);
	},

	cleanBindings: function (bindings) {
		return filter(bindings, function (binding) {
			return !(binding instanceof Expression);
		});
	},

	addBinding: function (value, type) {
		type = type || 'where';

		if(!this.bindings[type]) {
			throw new Error(`Invalid binding type: ${type}.`);
		}

		if(isArray(value)) {
			this.bindings[type] = values(this.bindings[type].concat(value));
		} else {
			this.bindings[type].push(value);
		}

		return this;
	},

	mergeBindings: function (query) {
		if(!query instanceof Builder) {
			throw new Error('Query must be an instance of Builder');
		}

		merge(this.bindings, query.bindings);

		return this;
	},

	setBindings: function (bindings, type) {
		type = type || 'where';

		if(!this.bindings.hasOwnProperty(type)) {
			throw new Error(`Invalid binding type: ${type}.`);
		}

		this.bindings[type] = bindings;

		return this;
	},

	getBindings: function () {
		var indexedBindings = {};

		var allBindings = [];
		forEach(this.bindings, function (binding) {
			allBindings = allBindings.concat(binding);
		});

		forEach(allBindings, function (binding, index) {
			indexedBindings[index] = binding;
		});

		return indexedBindings;
	},

	getGrammar: function () {
		return this.grammar;
	}
};