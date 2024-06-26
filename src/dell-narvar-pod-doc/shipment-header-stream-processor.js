/*
* File: src\dell-narvar-pod-doc\shipment-header-stream-processor.js
* Project: Omni-eventing-pub-sub-revamped
* Author: Bizcloud Experts
* Date: 2024-03-14
* Confidential and Proprietary
*/
'use strict';
const { get } = require('lodash');
const { publishToSNS, STATUSES, getCstTimestamp } = require('./helper');
const AWS = require('aws-sdk');
const { getStatusTableData } = require('./dynamo');

const dynamoDB = new AWS.DynamoDB.DocumentClient();

let functionName;
module.exports.handler = async (event, context) => {
  console.info('event:', JSON.stringify(event));
  functionName = context.functionName;
  try {
    await Promise.all(
      event.Records.map(async (record) => {
        const uploadDateTime = get(record, 'InsertedTimeStamp');
        if (uploadDateTime < '2024-03-12') {
          console.info(`UploadDateTime: ${uploadDateTime} is less than 2024-03-12. Skipping.`);
          return true;
        }
        const newImage = get(record, 'dynamodb.NewImage');
        const orderNo = get(newImage, 'PK_OrderNo.S', '');
        try {
          const existingItem = await getStatusTableData({ orderNo });
          console.info(
            '🙂 -> file: shipment-file-stream-processor.js:19 -> event.Records.map -> existingItem:',
            existingItem
          );
          if (existingItem.filter((item) => get(item, 'Status') === STATUSES.SENT).length > 0) {
            console.info(`Order no: ${orderNo} had already been processed.`);
            return `Order no: ${orderNo} had already been processed.`;
          }
          if (existingItem.length === 0) {
            console.info(`Order no: ${orderNo} not present in the status table.`);
            return `Order no: ${orderNo} not present in the status table.`;
          }
          const houseBill = get(newImage, 'Housebill.S', '');
          const customerRes = await getCustomer({ houseBill });
          console.info(
            '🙂 -> file: shipment-file-stream-processor.js:23 -> event.Records.map -> customerRes:',
            customerRes
          );
          if (customerRes.length === 0) {
            console.info(
              `Customer not present for housebill number in shipment entitlement table: ${houseBill} or the customer is not Dell`
            );
            await deleteDynamoRecord({ orderNo });
            return `Customer not present for housebill number in shipment entitlement table: ${houseBill} or the customer is not Dell`;
          }
          const customerIDs = customerRes.map((customer) => get(customer, 'CustomerID')).join();
          console.info(
            '🙂 -> file: shipment-file-stream-processor.js:25 -> event.Records.map -> customerIDs:',
            customerIDs
          );

          return await updateStatusTable({
            customerIDs,
            houseBill,
            orderNo,
            status: STATUSES.PENDING,
          });
        } catch (error) {
          const errorMessage = `Error details: ${error}. Order id: ${orderNo}`;
          console.error(errorMessage);
          await publishToSNS(errorMessage, context.functionName);
          return false;
        }
      })
    );
  } catch (error) {
    const errorMessage = `An error occurred in function ${context.functionName}. Error details: ${error}.`;
    console.error(errorMessage);
    await publishToSNS(errorMessage, context.functionName);
  }
};

async function getCustomer({ houseBill }) {
  const custIds = await getFilterPolicy();
  const filterExpression = custIds
    .map((_, index) => `CustomerID = :customer_id${index}`)
    .join(' OR ');

  const params = {
    TableName: process.env.CUSTOMER_ENTITLEMENT_TABLE,
    IndexName: process.env.ENTITLEMENT_HOUSEBILL_INDEX,
    KeyConditionExpression: 'HouseBillNumber = :Housebill',
    FilterExpression: filterExpression,
    ExpressionAttributeValues: {
      ':Housebill': houseBill,
    },
  };

  custIds.forEach((id, index) => {
    params.ExpressionAttributeValues[`:customer_id${index}`] = id;
    console.info('🙂 -> file: test.js:48 -> ExpressionAttributeValues:', params);
  });
  try {
    const data = await dynamoDB.query(params).promise();
    return get(data, 'Items', []);
  } catch (error) {
    console.error('Validation error:', error);
    return [];
  }
}

// Get customer ids for dell from filter policy of the subscription.
async function getFilterPolicy() {
  const subscriptionArn = process.env.SUBSCRIPTION_ARN;
  const sns = new AWS.SNS();
  const params = {
    SubscriptionArn: subscriptionArn,
  };
  const data = await sns.getSubscriptionAttributes(params).promise();
  const filterPolicy = data.Attributes.FilterPolicy;
  console.info('🙂 -> file: test.js:16 -> filterPolicy:', filterPolicy);
  const filterPolicyObject = JSON.parse(filterPolicy);
  const dellCustomerIds = get(filterPolicyObject, 'customer_id');
  console.info('🙂 -> file: test.js:20 -> dellCustomerIds:', dellCustomerIds);
  return dellCustomerIds;
}

async function deleteDynamoRecord({ orderNo }) {
  const params = {
    TableName: process.env.DOC_STATUS_TABLE,
    Key: { FK_OrderId: orderNo },
  };
  try {
    const data3 = await dynamoDB.delete(params).promise();
    console.info('🙂 -> file: test.js:176 -> deleteDynamoRecord -> data3:', data3);
    return true;
  } catch (error) {
    console.error('Validation error:', error);
    throw error;
  }
}

async function updateStatusTable({ orderNo, status, customerIDs, houseBill }) {
  try {
    const updateParam = {
      TableName: process.env.DOC_STATUS_TABLE,
      Key: { FK_OrderId: orderNo },
      UpdateExpression:
        'set #Status = :status, CustomerIDs = :customerIDs, HouseBill = :houseBill, RetryCount = :retryCount, LastUpdateBy = :lastUpdateBy, LastUpdatedAt = :lastUpdatedAt',
      ExpressionAttributeNames: { '#Status': 'Status' },
      ExpressionAttributeValues: {
        ':status': status,
        ':customerIDs': customerIDs,
        ':houseBill': houseBill,
        ':retryCount': 0,
        ':lastUpdateBy': functionName,
        ':lastUpdatedAt': getCstTimestamp(),
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
