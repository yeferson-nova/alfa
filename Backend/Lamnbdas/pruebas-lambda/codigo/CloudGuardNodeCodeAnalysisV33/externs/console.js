/**
 * @constructor
 * @see https://console.spec.whatwg.org/
 */
function Console() {};

/**
 * @param {*} condition
 * @param {...*} var_args
 * @return {undefined}
 */
Console.prototype.assert = function(condition, var_args) {};

/**
 * @param {...*} var_args
 * @return {undefined}
 */
Console.prototype.error = function(var_args) {};

/**
 * @param {...*} var_args
 * @return {undefined}
 */
Console.prototype.info = function(var_args) {};

/**
 * @param {...*} var_args
 * @return {undefined}
 */
Console.prototype.log = function(var_args) {};

/**
 * @param {...*} var_args
 * @return {undefined}
 */
Console.prototype.warn = function(var_args) {};

/**
 * @param {...*} var_args
 * @return {undefined}
 */
Console.prototype.debug = function(var_args) {};

/**
 * @param {*} value
 * @return {undefined}
 */
Console.prototype.dir = function(value) {};

/**
 * @param {...*} var_args
 * @return {undefined}
 */
Console.prototype.dirxml = function(var_args) {};

/**
 * @param {!Object} data
 * @param {*=} opt_columns
 * @return {undefined}
 */
Console.prototype.table = function(data, opt_columns) {};

/**
 * @param {...*} var_args
 * @return {undefined}
 */
Console.prototype.trace = function(var_args) {};

/**
 * @param {*} value
 * @return {undefined}
 */
Console.prototype.count = function(value) {};

/**
 * @param {*} value
 * @return {undefined}
 */
Console.prototype.markTimeline = function(value) {};

/**
 * @param {string=} opt_title
 * @return {undefined}
 */
Console.prototype.profile = function(opt_title) {};

/** @type {Array<ScriptProfile>} */
Console.prototype.profiles;

/**
 * @param {string=} opt_title
 * @return {undefined}
 */
Console.prototype.profileEnd = function(opt_title) {};

/**
 * @param {string} name
 * @return {undefined}
 */
Console.prototype.time = function(name) {};

/**
 * @param {string} name
 * @return {undefined}
 */
Console.prototype.timeEnd = function(name) {};

/**
 * @param {*} value
 * @return {undefined}
 */
Console.prototype.timeStamp = function(value) {};

/**
 * @param {...*} var_args
 * @return {undefined}
 */
Console.prototype.group = function(var_args) {};

/**
 * @param {...*} var_args
 * @return {undefined}
 */
Console.prototype.groupCollapsed = function(var_args) {};

Console.prototype.groupEnd = function() {};

Console.prototype.clear = function() {};

/** @type {MemoryInfo} */
Console.prototype.memory;

/** @type {!Console} */
Window.prototype.console;

/**
 * @type {!Console}
 * @suppress {duplicate}
 */
var console;