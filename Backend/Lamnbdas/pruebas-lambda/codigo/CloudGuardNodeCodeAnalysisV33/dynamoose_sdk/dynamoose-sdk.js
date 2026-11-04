ProtegoAWSNamespace.AWS = ProtegoAWSNamespace;
ProtegoAWSNamespace.Dynamoose = ProtegoAWSNamespace;

/** 
 * @constructor
 * @param {Object=} params
 */
ProtegoAWSNamespace.model = function (params) { 
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;describeTable;dynamodb:describeTable" + JSON.stringify(params) + "__protego_api_usage_end__");
    return ProtegoAWSNamespace.model; 
};

/** @this {ProtegoAWSNamespace.model} */
ProtegoAWSNamespace.model.prototype.put = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;putItem;dynamodb:PutItem" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.model} */
ProtegoAWSNamespace.model.prototype.save = function(params, callback) {
   console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;putItem;dynamodb:PutItem" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.model} */
ProtegoAWSNamespace.model.prototype.batchPut = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;batchWriteItem;dynamodb:BatchWriteItem" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.model} */
ProtegoAWSNamespace.model.prototype.create = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;putItem;dynamodb:PutItem" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.model} */
ProtegoAWSNamespace.model.prototype.get = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;getItem;dynamodb:GetItem" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.model} */
ProtegoAWSNamespace.model.prototype.update = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;updateItem;dynamodb:updateItem" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.model} */
ProtegoAWSNamespace.model.prototype.batchGet = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;batchGetItem;dynamodb:BatchGetItem" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.model} */
ProtegoAWSNamespace.model.prototype.batchDelete = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;batchWriteItem;dynamodb:BatchWriteItem" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.model} */
ProtegoAWSNamespace.model.prototype.delete = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;deleteItem;dynamodb:DeleteItem" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.model} */
ProtegoAWSNamespace.model.prototype.scan = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;scan;dynamodb:Scan" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.model} */
ProtegoAWSNamespace.model.prototype.query = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;query;dynamodb:Query" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.model} */
ProtegoAWSNamespace.model.prototype.queryOne = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;query;dynamodb:Query" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** 
 * @constructor
 * @param {Object=} params
 */
ProtegoAWSNamespace.Table = function (params) { return ProtegoAWSNamespace.Table; };

/** @this {ProtegoAWSNamespace.Table} */
ProtegoAWSNamespace.Table.prototype.createIndex = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;updateTable;dynamodb:updateTable" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.Table} */
ProtegoAWSNamespace.Table.prototype.deleteIndex = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;updateTable;dynamodb:updateTable" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.Table} */
ProtegoAWSNamespace.Table.prototype.describeTTL = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;describeTimeToLive;dynamodb:describeTimeToLive" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.Table} */
ProtegoAWSNamespace.Table.prototype.updateTTL = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;updateTimeToLive;dynamodb:updateTimeToLive" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.Table} */
ProtegoAWSNamespace.Table.prototype.describe = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;describeTable;dynamodb:describeTable" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.Table} */
ProtegoAWSNamespace.Table.prototype.create = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;createTable;dynamodb:createTable" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};

/** @this {ProtegoAWSNamespace.Table} */
ProtegoAWSNamespace.Table.prototype.delete = function(params, callback) {
    console.log( "__protego_api_usage__;DynamoDB;ProtegoAWSNamespace.DynamoDB;deleteTable;dynamodb:deleteTable" + JSON.stringify(params) + "__protego_api_usage_end__");
    callback(null, 'ProtegoData');
};
