# sqlbuilder

Laravel 4+ query builder ported to Node.js.

## Installation
```
npm install --save sqlbuilder
```

## Usage

Do what ever you want with your query:

*Example 1*
```js
var builder = require('sqlbuilder')({
	host: 'localhost',
	user: 'root',
	password: '',
	database: 'mydb'
});

builder
.select('*').from('users')
.whereYear('created_at', '=', 2014)
.toSql(); // select * from `users` where year(`created_at`) = ?

builder.get().then(function (response) {
	var rows = response.rows,
			fields = response.fields;

	res.json(rows);
});
```