var MockConnection = require('../helpers/mock-connection');
var mockConnection = new MockConnection();

describe('MockConnection', function () {
	it('should expect a query', function () {
		mockConnection.expectQuery('select * from `products`');
		mockConnection.executeQuery('select * from `products`').then(function (response) {
			assert.equal(undefined, response.rows);
		});
	});

	it('should expect a query with a custom response', function () {
		mockConnection.expectQuery('select * from `products`').respond([
			{id: 1, name: 'Product 1'},
			{id: 2, name: 'Product 2'}
		]);
		mockConnection.executeQuery('select * from `products`').then(function (response) {
			assert.equal(1, response.rows[0].id);
			assert.equal(2, response.rows[1].id);

			assert.equal('Product 1', response.rows[0].name);
			assert.equal('Product 2', response.rows[1].name);
		});
	});
});