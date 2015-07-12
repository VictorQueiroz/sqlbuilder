module.exports = Expression;

function Expression(value) {
	this.value = value;
}

Expression.prototype = {
	getValue: function () {
		return this.value;
	},

	toString: function () {
		return this.getValue();
	}
};