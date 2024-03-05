'use strict';

const { get, pickBy } = require('lodash');
const { STATUSES, tableParams, publishToSNS } = require('./helper');
const AWS = require('aws-sdk');
const moment = require('moment-timezone');

const dynamoDB = new AWS.DynamoDB.DocumentClient();

let functionName;

module.exports.handler = async (event, context) => {
  functionName = context.functionName;
  console.info('event:', JSON.stringify(event));
  try {
    const getPendingRecordsRes = await getPendingRecords();
    console.info(
      '🙂 -> file: table-status-checker.js:13 -> module.exports.handler= -> getPendingRecordsRes:',
      getPendingRecordsRes
    );

    await Promise.all(
      getPendingRecordsRes.map(async (record) => {
        const orderNo = get(record, 'FK_OrderId', '');
        const retryCount = get(record, 'RetryCount', 0);
        try {
          const tableStatuses = get(record, 'TableStatuses', {});
          console.info(
            '🙂 -> file: table-status-checker.js:17 -> awaitPromise.all -> tableStatuses:',
            tableStatuses
          );
          const originalTableStatuses = { ...tableStatuses };

          const pendingTableStatuses = pickBy(tableStatuses, (value) => value === STATUSES.PENDING);
          console.info(
            '🙂 -> file: table-status-checker.js:19 -> awaitPromise.all -> pendingTableStatuses:',
            pendingTableStatuses
          );
          await Promise.all(
            Object.keys(pendingTableStatuses).map(async (tableName) => {
              const tableParam = tableParams[tableName]({ orderNo });
              console.info(
                '🙂 -> file: table-status-checker.js:25 -> awaitPromise.all -> tableParam:',
                tableParam
              );
              const checkTableDataRes = await checkTableData({ params: tableParam });
              console.info(
                '🙂 -> file: table-status-checker.js:27 -> awaitPromise.all -> checkTableDataRes:',
                checkTableDataRes
              );
              originalTableStatuses[tableName] = checkTableDataRes;
            })
          );
          console.info(
            '🙂 -> file: table-status-checker.js:21 -> awaitPromise.all -> originalTableStatuses:',
            originalTableStatuses
          );

          if (Object.values(originalTableStatuses).includes(STATUSES.PENDING)) {
            await updateStatusTable({
              orderNo,
              originalTableStatuses,
              retryCount,
              status: STATUSES.PENDING,
            });
          }

          if (!Object.values(originalTableStatuses).includes(STATUSES.PENDING)) {
            await updateStatusTable({
              orderNo,
              originalTableStatuses,
              retryCount,
              status: STATUSES.READY,
            });
          }

          const pendingStatuses = pickBy(
            originalTableStatuses,
            (value) => value === STATUSES.PENDING
          );
          if (
            Object.keys(pendingStatuses).length === 1 &&
            get(originalTableStatuses, 'SHIPMENT_MILESTONE_TABLE', null) === STATUSES.PENDING &&
            retryCount >= 5
          ) {
            await updateStatusTable({
              orderNo,
              originalTableStatuses,
              retryCount,
              status: STATUSES.SKIPPED,
              message: `Shipment milestone table is not populated for Order id: ${orderNo} and Status id: DEL`,
            });
          }

          if (Object.values(originalTableStatuses).includes(STATUSES.PENDING) && retryCount >= 5) {
            await updateStatusTable({
              orderNo,
              originalTableStatuses,
              retryCount,
              status: STATUSES.FAILED,
              message: `Tables are not populated for Order id: ${orderNo}\n
              Pending tables: ${JSON.stringify(pickBy(originalTableStatuses, (value) => value === STATUSES.PENDING))}\n
            `,
            });
          }
        } catch (error) {
          console.info(
            '🙂 -> file: table-status-checker.js:100 -> getPendingRecordsRes.map -> error:',
            error
          );
          await updateStatusTableStatus({
            orderNo,
            status: STATUSES.FAILED,
            message: error.message,
          });
        }
      })
    );
  } catch (error) {
    const errorMessage = `An error occurred in function ${context.functionName}. Error details: ${error}.`;
    console.error(errorMessage);
    await publishToSNS(errorMessage, context.functionName);
  }
};

async function getPendingRecords() {
  const params = {
    TableName: process.env.DOC_STATUS_TABLE,
    IndexName: process.env.DOC_STATUS_INDEX,
    KeyConditionExpression: '#Status = :status',
    ExpressionAttributeNames: { '#Status': 'Status' },
    ExpressionAttributeValues: {
      ':status': STATUSES.PENDING,
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

async function checkTableData({ params }) {
  try {
    const data = await dynamoDB.query(params).promise();
    return get(data, 'Items', []).length > 0 ? STATUSES.READY : STATUSES.PENDING;
  } catch (error) {
    console.error('Validation error:', error);
    return false;
  }
}

async function updateStatusTable({ orderNo, originalTableStatuses, retryCount, status }) {
  try {
    const updateParam = {
      TableName: process.env.DOC_STATUS_TABLE,
      Key: { FK_OrderId: orderNo },
      UpdateExpression:
        'set TableStatuses = :tableStatuses, RetryCount = :retryCount, #Status = :status, LastUpdateBy = :lastUpdateBy, LastUpdatedAt = :lastUpdatedAt',
      ExpressionAttributeNames: { '#Status': 'Status' },
      ExpressionAttributeValues: {
        ':tableStatuses': originalTableStatuses,
        ':retryCount': retryCount + 1,
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

async function updateStatusTableStatus({ orderNo, status, message }) {
  try {
    const updateParam = {
      TableName: process.env.DOC_STATUS_TABLE,
      Key: { FK_OrderId: orderNo },
      UpdateExpression:
        'set #Status = :status, LastUpdateBy = :lastUpdateBy, LastUpdatedAt = :lastUpdatedAt, Message = :message',
      ExpressionAttributeNames: { '#Status': 'Status' },
      ExpressionAttributeValues: {
        ':status': status,
        ':lastUpdateBy': functionName,
        ':lastUpdatedAt': moment.tz('America/Chicago').format(),
        ':message': message,
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
