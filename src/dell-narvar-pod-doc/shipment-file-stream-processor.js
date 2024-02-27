'use strict';
const { get } = require('lodash');
const { publishToSNS, tableStatuses, STATUSES, getCstTimestamp } = require('./helper');
const AWS = require('aws-sdk');

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
            `Customer not present for housebill number in shipment entitlement table: ${houseBill}`
          );
          return await insertIntoDocStatusTable({
            houseBill,
            message: `Customer not present for housebill number in shipment entitlement table: ${houseBill}`,
            orderNo,
            status: STATUSES.SKIPPED,
          });
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
          status: STATUSES.PENDING,
        });
      })
    );
  } catch (error) {
    const errorMessage = `An error occurred in function ${context.functionName}. Error details: ${error}.`;
    console.error(errorMessage);
    await publishToSNS(errorMessage, context.functionName);
  }
};

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

async function getCustomer({ houseBill }) {
  const params = {
    TableName: process.env.CUSTOMER_ENTITLEMENT_TABLE,
    IndexName: process.env.ENTITLEMENT_HOUSEBILL_INDEX,
    KeyConditionExpression: 'HouseBillNumber = :Housebill',
    ExpressionAttributeValues: {
      ':Housebill': houseBill,
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

async function insertIntoDocStatusTable({ orderNo, houseBill, customerIDs, status, message }) {
  try {
    const insertParams = {
      TableName: process.env.DOC_STATUS_TABLE,
      Item: {
        FK_OrderId: orderNo,
        Status: status,
        HouseBill: houseBill,
        CustomerIDs: customerIDs,
        RetryCount: 0,
        Payload: '',
        Message: message,
        CreatedAt: getCstTimestamp(),
        LastUpdatedAt: getCstTimestamp(),
        LastUpdatedBy: functionName,
      },
    };
    console.info(
      '🙂 -> file: shipment-file-stream-processor.js:94 -> insertIntoDocStatusTable -> insertParams:',
      insertParams
    );
    if (status === STATUSES.PENDING) {
      insertParams.Item.TableStatuses = tableStatuses;
    }
    return await dynamoDB.put(insertParams).promise();
  } catch (error) {
    throw new Error(
      `Error inserting into doc status table. Order id: ${orderNo}, message: ${error.message}`
    );
  }
}
