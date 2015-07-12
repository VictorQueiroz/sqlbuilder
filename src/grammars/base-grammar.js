var _ = require('lodash'),
		map = _.map,
		forEach = _.forEach;

var Expression = require('../expression');

function BaseGrammar() {
	this.tablePrefix = '';
}

BaseGrammar.prototype = {
	wrapArray: function (values) {
		return map(values, this.wrap, this);
	},
	wrapTable: function (table) {
		if(this.isExpression(table)) {
			return this.getValue(table);
		}

		return this.wrap(`${this.tablePrefix}${table}`, true);
	},
	wrap: function (value, prefixAlias) {
    prefixAlias = prefixAlias || false;

		if(this.isExpression(value)) {
			return this.getValue(value);
		}

		// If the value being wrapped has a column alias we will need to separate out
    // the pieces so we can wrap each of the segments of the expression on it
    // own, and then joins them both back together with the "as" connector.
    if(value.toLowerCase().indexOf(' as ') > -1) {
    	var segments = value.split(' ');

    	if(prefixAlias) {
    		segments[2] = `${this.tablePrefix}${segments[2]}`;
    	}

    	return `${this.wrap(segments[0])} as ${this.wrapValue(segments[2])}`;
    }

    var wrapped = [];

    var segments = value.split('.');

    // If the value is not an aliased table expression, we'll just wrap it like
    // normal, so if there is more than one segment, we will wrap the first
    // segments as if it was a table and the rest as just regular values.
    forEach(segments, function (segment, key) {
    	if(key == 0 && segments.length > 1) {
    		wrapped.push(this.wrapTable(segment));
    	} else {
    		wrapped.push(this.wrapValue(segment));
    	}
    }, this);

    return wrapped.join('.');
	},
	/**
   * Wrap a single string in keyword identifiers.
   */
  wrapValue: function (value) {
  	if(value === '*') {
  		return value;
  	}

  	return value && '"' + value.replace('"', '""') + '"' || '';
  },

  /**
	 * Convert an array of column names into a delimited string.
	 */
  columnize: function (columns) {
  	return map(columns, this.wrap, this).join(', ');
  },

  parameterize: function (values) {
  	return map(values, this.parameter, this).join(', ');
  },

  parameter: function (value) {
  	return this.isExpression(value) ? this.getValue(value) : '?';
  },

  getValue: function (expression) {
  	return expression.getValue();
  },

  isExpression: function(value) {
  	return value instanceof Expression;
  },

  getDateFormat: function () {
  	return 'Y-m-d H:i:s';
  },

  getTablePrefix: function () {
  	return this.tablePrefix;
  },

  setTablePrefix: function (tablePrefix) {
  	this.tablePrefix = tablePrefix;

  	return this;
  }
};

module.exports = BaseGrammar;