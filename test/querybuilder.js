var assert = require('assert');
var QueryBuilder = require('../src/querybuilder');
var Grammar = require('../src/grammars/grammar');
var MySqlGrammar = require('../src/grammars/mysql-grammar');

var MockConnection = require('../helpers/mock-connection');
var mockConnection = new MockConnection();

describe('QueryBuilder', function () {
	var builder,
			mySqlBuilder;

	beforeEach(function () {
		builder = new QueryBuilder(mockConnection, new Grammar());
		mySqlBuilder = new QueryBuilder(mockConnection, new MySqlGrammar());
	});

	it('should set the columns to be selected', function () {
		builder.select('*').from('users');
		assert.equal('select * from "users"', builder.toSql());
	});

	it('should wrap quotation marks', function () {
		builder.select('*').from('some"table');
		assert.equal('select * from "some""table"', builder.toSql());
	});

	it('should wrap alias as whole constant', function () {
		builder.select('x.y as foo.bar').from('baz');
		assert.equal('select "x"."y" as "foo.bar" from "baz"', builder.toSql());
	});

	it('should add multiple selects', function () {
		builder.select('foo').addSelect('bar').addSelect(['baz', 'boom']).from('users');
		assert.equal('select "foo", "bar", "baz", "boom" from "users"', builder.toSql());
	});

	it('should add select with prefix', function () {
		builder.getGrammar().setTablePrefix('prefix_');
		builder.select('*').from('users');
		assert.equal('select * from "prefix_users"', builder.toSql());
	});

	it('should perform distinct selects', function () {
		builder.distinct().select('foo', 'bar').from('users');
	  assert.equal('select distinct "foo", "bar" from "users"', builder.toSql());
	});

	it('should allow basic alias', function () {
		builder.select('foo as bar').from('users');
		assert.equal('select "foo" as "bar" from "users"', builder.toSql());
	});

	it('should allow basic alias with prefix', function () {
		builder.getGrammar().setTablePrefix('prefix_');
		builder.select('*').from('users as people');
		assert.equal('select * from "prefix_users" as "prefix_people"', builder.toSql());
	});

	it('should join aliases with prefix', function () {
		builder.getGrammar().setTablePrefix('prefix_');
		builder.select('*').from('services').join('translations AS t', 't.item_id', '=', 'services.id');
		assert.equal('select * from "prefix_services" inner join "prefix_translations" as "prefix_t" on "prefix_t"."item_id" = "prefix_services"."id"', builder.toSql());
	});

	it('should perform basic wrap table', function () {
		builder.select('*').from('public.users');
		assert.equal('select * from "public"."users"', builder.toSql());
	});

	it('should perform basic wheres', function () {
		builder.select('*').from('users').where('id', '=', 1);
		assert.equal('select * from "users" where "id" = ?', builder.toSql());
		assert.deepEqual({0: 1}, builder.getBindings())
	});

	it('should perform mysql wrapping protects quotation marks', function () {
		mySqlBuilder.select('*').from('some`table');
		assert.equal('select * from `some``table`', mySqlBuilder.toSql());
	});

	it('should perform where day mysql search', function () {
		var builder = mySqlBuilder;
		builder.select('*').from('users').whereDay('created_at', '=', 1);
		assert.equal('select * from `users` where day(`created_at`) = ?', builder.toSql());
		assert.deepEqual({0: 1}, builder.getBindings());
	});

	it('should perform where month mysql search', function () {
		var builder = mySqlBuilder;
    builder.select('*').from('users').whereMonth('created_at', '=', 5);
    assert.equal('select * from `users` where month(`created_at`) = ?', builder.toSql());
    assert.deepEqual({0 : 5}, builder.getBindings());
	});

	it('should perform where year mysql search', function () {
		var builder = mySqlBuilder;
    builder.select('*').from('users').whereYear('created_at', '=', 2014);
    assert.equal('select * from `users` where year(`created_at`) = ?', builder.toSql());
    assert.deepEqual({0 : 2014}, builder.getBindings());
	});

	it('should perform insert', function (done) {
		builder.from('users').insert([
			{ name: 'foo', email: 'bar', bio: 'about he' }
		]).then(function () {
			done();
		});
	});
});