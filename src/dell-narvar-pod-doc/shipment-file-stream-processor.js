'use strict';
const { get } = require('lodash');
const { publishToSNS, tableStatuses, STATUSES, getCstTimestamp } = require('./helper');
const AWS = require('aws-sdk');
const { getShipmentHeaderData, getStatusTableData } = require('./dynamo');

const dynamoDB = new AWS.DynamoDB.DocumentClient();

let functionName;
module.exports.handler = async (event, context) => {
  console.info('event:', JSON.stringify(event));
  functionName = context.functionName;
  try {
    await Promise.all(
      event.Records.map(async (record) => {
        const uploadDateTime = get(record, 'UploadDateTime');
        if (uploadDateTime < '2024-03-04') {
          console.info(`UploadDateTime: ${uploadDateTime} is less than 2024-03-04. Skipping.`);
          return true;
        }
        const newImage = get(record, 'dynamodb.NewImage');
        const orderNo = get(newImage, 'FK_OrderNo.S', '');
        const docType = get(newImage, 'FK_DocType.S', '');
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
          const shipmentHeaderDataRes = await getShipmentHeaderData({ orderNo });
          console.info(
            '🙂 -> file: shipment-file-stream-processor.js:17 -> event.Records.map -> shipmentHeaderDataRes:',
            shipmentHeaderDataRes
          );
          const houseBill = get(shipmentHeaderDataRes, 'Housebill', null);
          if (!houseBill) {
            console.info(
              `Housebill number not present for order id in shipment header table: ${orderNo}`
            );
            return await insertIntoDocStatusTable({
              orderNo,
              status: STATUSES.SKIPPED,
              docType,
              message: `Housebill number not present for order id in shipment header table: ${orderNo}`,
            });
          }
          const customerRes = await getCustomer({ houseBill });
          console.info(
            '🙂 -> file: shipment-file-stream-processor.js:23 -> event.Records.map -> customerRes:',
            customerRes
          );
          if (customerRes.length === 0) {
            console.info(
              `Customer not present for housebill number in shipment entitlement table: ${houseBill} or the customer is not Dell`
            );
            return `Customer not present for housebill number in shipment entitlement table: ${houseBill} or the customer is not Dell`;
          }
          const customerIDs = customerRes.map((customer) => get(customer, 'CustomerID')).join();
          console.info(
            '🙂 -> file: shipment-file-stream-processor.js:25 -> event.Records.map -> customerIDs:',
            customerIDs
          );
          return await insertIntoDocStatusTable({
            customerIDs,
            houseBill,
            orderNo,
            docType,
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

async function insertIntoDocStatusTable({
  orderNo,
  houseBill,
  customerIDs,
  status,
  message,
  docType,
}) {
  try {
    const insertParams = {
      TableName: process.env.DOC_STATUS_TABLE,
      Item: {
        FK_OrderId: orderNo,
        Status: status,
        HouseBill: houseBill,
        CustomerIDs: customerIDs,
        DocType: docType,
        RetryCount: 0,
        Payload: '',
        Message: message,
        CreatedAt: getCstTimestamp(),
        LastUpdatedAt: getCstTimestamp(),
        LastUpdateBy: functionName,
      },
    };
    if (status === STATUSES.PENDING) {
      insertParams.Item.TableStatuses = tableStatuses;
    }
    console.info(
      '🙂 -> file: shipment-file-stream-processor.js:129 -> insertIntoDocStatusTable -> insertParams:',
      insertParams
    );
    return await dynamoDB.put(insertParams).promise();
  } catch (error) {
    throw new Error(
      `Error inserting into doc status table. Order id: ${orderNo}, message: ${error.message}`
    );
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
