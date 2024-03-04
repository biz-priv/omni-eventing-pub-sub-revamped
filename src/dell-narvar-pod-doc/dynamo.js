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
    const result = await dynamoDB.query(params).promise();
    return get(result, 'Items.[0]', {});
  } catch (error) {
    console.error('Error querying header details:', error.message);
    return false;
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
    const data = await dynamoDB.query(params).promise();
    return get(data, 'Items', []);
  } catch (error) {
    console.error('Validation error:', error);
    return false;
  }
}

module.exports = { getShipmentHeaderData, getStatusTableData };
