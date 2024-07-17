/*
 * File: src\dell-narvar-pod-doc\helper.js
 * Project: Omni-eventing-pub-sub-revamped
 * Author: Bizcloud Experts
 * Date: 2024-03-14
 * Confidential and Proprietary
 */
'use strict';
const AWS = require('aws-sdk');
const moment = require('moment-timezone');

const { REGION } = process.env;

const STATUSES = {
  PENDING: 'PENDING',
  READY: 'READY',
  FAILED: 'FAILED',
  SENT: 'SENT',
  SKIPPED: 'SKIPPED',
};

const tableStatuses = {
  SHIPMENT_HEADER_TABLE: STATUSES.READY,
  SHIPMENT_MILESTONE_TABLE: STATUSES.PENDING,
  CONSIGNEE_TABLE: STATUSES.PENDING,
  SHIPPER_TABLE: STATUSES.PENDING,
};

async function publishToSNS(message, subject) {
  const sns = new AWS.SNS({ region: REGION });

  const params = {
    Message: `${message} \n Retrigger process: Set the status to PENDING and ResetCount to 0 of that particular record in ${process.env.DOC_STATUS_TABLE}`,
    Subject: `Lambda function ${subject} has failed.`,
    TopicArn: process.env.ERROR_SNS_TOPIC_ARN,
  };
  try {
    await sns.publish(params).promise();
  } catch (error) {
    console.error(error);
  }
}

function getCstTimestamp(date = new Date()) {
  return moment(date).tz('America/Chicago').format('YYYY:MM:DD HH:mm:ss Z');
}

const tableParams = {
  SHIPMENT_MILESTONE_TABLE: ({ orderNo }) => ({
    TableName: process.env.SHIPMENT_MILESTONE_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo and FK_OrderStatusId = :statusId',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
      ':statusId': 'DEL',
    },
  }),
  SHIPPER_TABLE: ({ orderNo }) => ({
    TableName: process.env.SHIPPER_TABLE,
    KeyConditionExpression: 'FK_ShipOrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  }),
  CONSIGNEE_TABLE: ({ orderNo }) => ({
    TableName: process.env.CONSIGNEE_TABLE,
    KeyConditionExpression: 'FK_ConOrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  }),
};

module.exports = { STATUSES, tableStatuses, publishToSNS, getCstTimestamp, tableParams };
