'use strict';
const { get } = require('lodash');
const { publishToSNS, STATUSES } = require('./helper');
const AWS = require('aws-sdk');
const moment = require('moment-timezone');
const { getStatusTableData } = require('./dynamo');

const dynamoDB = new AWS.DynamoDB.DocumentClient();

let functionName;
module.exports.handler = async (event, context) => {
  console.info('event:', JSON.stringify(event));
  functionName = context.functionName;
  try {
    await Promise.all(
      event.Records.map(async (record) => {
        const newImage = get(record, 'dynamodb.NewImage');
        const orderNo = get(newImage, 'FK_OrderNo.S', '');
        const existingItem = await getStatusTableData({ orderNo });
        console.info(
          '🙂 -> file: shipment-file-stream-processor.js:19 -> event.Records.map -> existingItem:',
          existingItem
        );
        if (existingItem.filter((item) => get(item, 'Status') === STATUSES.SENT).length > 0) {
          console.info(`Order no: ${orderNo} had already been processed.`);
          return `Order no: ${orderNo} had already been processed.`;
        }
        if (existingItem.filter((item) => get(item, 'Status') !== STATUSES.SENT).length > 0) {
          return await updateStatusTable({ orderNo, status: STATUSES.PENDING });
        }
        return true;
      })
    );
  } catch (error) {
    const errorMessage = `An error occurred in function ${context.functionName}. Error details: ${error}.`;
    console.error(errorMessage);
    await publishToSNS(errorMessage, context.functionName);
  }
};

async function updateStatusTable({ orderNo, status }) {
  try {
    const updateParam = {
      TableName: process.env.DOC_STATUS_TABLE,
      Key: { FK_OrderId: orderNo },
      UpdateExpression:
        'set #Status = :status, LastUpdateBy = :lastUpdateBy, LastUpdatedAt = :lastUpdatedAt',
      ExpressionAttributeNames: { '#Status': 'Status' },
      ExpressionAttributeValues: {
        ':status': status,
        ':lastUpdateBy': functionName,
        ':lastUpdatedAt': moment.tz('America/Chicago').format(),
      },
    };
    console.info(
      '🙂 -> file: table-status-checker.js:79 -> updateStatusTable -> updateParam:',
      updateParam
    );
    return await dynamoDB.update(updateParam).promise();
  } catch (error) {
    console.info('🙂 -> file: table-status-checker.js:82 -> updateStatusTable -> error:', error);
    throw error;
  }
}
