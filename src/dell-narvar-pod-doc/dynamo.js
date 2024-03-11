'use strict';
const AWS = require('aws-sdk');
const { get } = require('lodash');

const dynamoDB = new AWS.DynamoDB.DocumentClient();

async function getShipmentHeaderData({ orderNo }) {
  const params = {
    TableName: process.env.SHIPMENT_HEADER_TABLE,
    KeyConditionExpression: 'PK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };
  try {
    const result = await dbRead(params);
    return get(result, 'Items.[0]', {});
  } catch (error) {
    console.error('Error querying header details:', error.message);
    return {};
  }
}

async function getStatusTableData({ orderNo }) {
  const params = {
    TableName: process.env.DOC_STATUS_TABLE,
    KeyConditionExpression: 'FK_OrderId = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };
  try {
    const data = await dbRead(params);
    return get(data, 'Items', []);
  } catch (error) {
    console.error('Validation error:', error);
    return [];
  }
}

async function dbRead(params) {
  let scanResults = [];
  let items;
  try {
    do {
      console.info('dbRead > params ', params);
      items = await dynamoDB.query(params).promise();
      scanResults = scanResults.concat(get(items, 'Items', []));
      params.ExclusiveStartKey = get(items, 'LastEvaluatedKey');
    } while (typeof items.LastEvaluatedKey !== 'undefined');
  } catch (e) {
    console.error('DynamoDb scan error. ', ' Params: ', params, ' Error: ', e);
    throw e;
  }
  return { Items: scanResults };
}

module.exports = { getShipmentHeaderData, getStatusTableData };
