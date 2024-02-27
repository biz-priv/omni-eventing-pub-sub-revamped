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
    Message: message,
    Subject: `Lambda function ${subject} has failed.`,
    TopicArn: process.env.ERROR_SNS_ARN,
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

module.exports = { STATUSES, tableStatuses, publishToSNS, getCstTimestamp };
