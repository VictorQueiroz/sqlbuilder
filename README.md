# sqlbuilder

Laravel 4+ query builder ported to Node.js.

## Installation
```
npm install --save sqlbuilder
```

## Usage

Do what ever you want with your query:

```js
var sqlbuilder = require('sqlbuilder');
var MySqlGrammar = sqlbuilder.MySqlGrammar;
var QueryBuilder = sqlbuilder.QueryBuilder;

var mySqlBuilder = new QueryBuilder(new MySqlGrammar());

var sql = mysqlBuilder.select('*').from('users').whereYear('created_at', '=', 2014).toSql();
```