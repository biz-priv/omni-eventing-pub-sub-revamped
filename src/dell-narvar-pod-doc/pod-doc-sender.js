'use strict';
const { get } = require('lodash');
const { publishToSNS, STATUSES } = require('./helper');
const { getShipmentHeaderData } = require('./dynamo');
const AWS = require('aws-sdk');
const Joi = require('joi');
const { v4 } = require('uuid');
const axios = require('axios').default;
const moment = require('moment-timezone');

const sns = new AWS.SNS({ apiVersion: '2010-03-31' });
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const S3 = new AWS.S3();

let functionName;

module.exports.handler = async (event, context) => {
  functionName = context.functionName;
  console.info('event:', JSON.stringify(event));
  try {
    await Promise.all(
      event.Records.map(async (record) => {
        console.info('🙂 -> file: pod-doc-sender.js:14 -> event.Records.map -> record:', record);
        const newImage = get(record, 'dynamodb.NewImage');
        console.info(
          '🙂 -> file: pod-doc-sender.js:16 -> event.Records.map -> newImage:',
          newImage
        );
        const orderNo = get(newImage, 'FK_OrderId.S', '');
        console.info('🙂 -> file: pod-doc-sender.js:17 -> event.Records.map -> orderNo:', orderNo);
        const customerIDs = get(newImage, 'CustomerIDs.S', '');
        console.info(
          '🙂 -> file: pod-doc-sender.js:18 -> event.Records.map -> customerIDs:',
          customerIDs
        );
        const payload = await createPayload({ orderNo });
        console.info('🙂 -> file: pod-doc-sender.js:27 -> event.Records.map -> payload:', payload);
        const { value, error } = schema.validate(payload);
        console.info(
          '🙂 -> file: pod-doc-sender.js:31 -> event.Records.map ->  value, error:',
          value,
          error
        );
        if (error) {
          throw new Error(`Payload validation failed: ${get(error, 'message')}`);
        }
        await Promise.all(
          customerIDs.split().map(async (customerId) => {
            await processAndDeliverMessage({ customerId, payload });
          })
        );
        await updateStatusTable({ orderNo, status: STATUSES.SENT });
      })
    );
  } catch (error) {
    const errorMessage = `An error occurred in function ${functionName}. Error details: ${error}.`;
    console.error(errorMessage);
    await publishToSNS(errorMessage, functionName);
  }
};

async function queryShipperDetails({ orderNo }) {
  const params = {
    TableName: process.env.SHIPPER_TABLE,
    KeyConditionExpression: 'FK_ShipOrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };
  try {
    const result = await dynamoDB.query(params).promise();
    return get(result, 'Items.[0]', false);
  } catch (error) {
    console.error('Error querying shipper details:', error.message);
    return false;
  }
}

async function queryConsigneeDetails({ orderNo }) {
  const params = {
    TableName: process.env.CONSIGNEE_TABLE,
    KeyConditionExpression: 'FK_ConOrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };
  try {
    const result = await dynamoDB.query(params).promise();
    return get(result, 'Items.[0]', false);
  } catch (error) {
    console.error('Error querying consignee details:', error.message);
    return false;
  }
}

async function queryMilestoneDetails({ orderNo }) {
  const params = {
    TableName: process.env.SHIPMENT_MILESTONE_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo and FK_OrderStatusId = :statusId',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
      ':statusId': 'DEL',
    },
  };
  try {
    const result = await dynamoDB.query(params).promise();
    return get(result, 'Items.[0]', false);
  } catch (error) {
    console.error('Error querying consignee details:', error.message);
    return false;
  }
}

const schema = Joi.object({
  id: Joi.string().required(),
  trackingNo: Joi.string().required(),
  carrier: Joi.string().required(),
  statusCode: Joi.string().required(),
  lastUpdateDate: Joi.string().required(),
  estimatedDeliveryDate: Joi.string().required(),
  identifier: Joi.string().required(),
  statusDescription: Joi.string().required(),
  retailerMoniker: Joi.string().required(),
  originCity: Joi.string().required(),
  originState: Joi.string().required(),
  originZip: Joi.string().required(),
  originCountryCode: Joi.string().required(),
  destCity: Joi.string().required(),
  destState: Joi.string().required(),
  destZip: Joi.string().required(),
  destCountryCode: Joi.string().required(),
  eventCity: Joi.string().required(),
  eventState: Joi.string().required(),
  eventZip: Joi.string().required(),
  eventCountryCode: Joi.string().required(),
  vpod: Joi.string().required(),
});

async function createPayload({ orderNo }) {
  const shipmentHeaderDataRes = await getShipmentHeaderData({ orderNo });
  console.info(
    '🙂 -> file: shipment-file-stream-processor.js:17 -> event.Records.map -> shipmentHeaderDataRes:',
    shipmentHeaderDataRes
  );
  const houseBill = get(shipmentHeaderDataRes, 'Housebill', null);
  console.info('🙂 -> file: pod-doc-sender.js:23 -> event.Records.map -> houseBill:', houseBill);
  const etaDateTime = get(shipmentHeaderDataRes, 'ETADateTime', null);
  console.info(
    '🙂 -> file: pod-doc-sender.js:29 -> event.Records.map -> etaDateTime:',
    etaDateTime
  );
  const shipperDetails = await queryShipperDetails({ orderNo });
  console.info(
    '🙂 -> file: pod-doc-sender.js:34 -> event.Records.map -> shipperDetails:',
    shipperDetails
  );
  const consigneeDetails = await queryConsigneeDetails({ orderNo });
  console.info(
    '🙂 -> file: pod-doc-sender.js:36 -> event.Records.map -> consigneeDetails:',
    consigneeDetails
  );
  const milestoneDetails = await queryMilestoneDetails({ orderNo });
  console.info(
    '🙂 -> file: pod-doc-sender.js:40 -> event.Records.map -> milestoneDetails:',
    milestoneDetails
  );
  const { FK_OrderStatusId: orderStatusId, EventDateTime: eventDateTime } = milestoneDetails;
  const payload = {
    id: v4(),
    trackingNo: houseBill,
    carrier: get(shipperDetails, 'ShipName', ''),
    statusCode: orderStatusId,
    lastUpdateDate: eventDateTime,
    estimatedDeliveryDate: etaDateTime === '1900-01-01 00:00:00.000' ? 'NA' : etaDateTime,
    identifier: 'NA',
    statusDescription: 'POD Document',
    retailerMoniker: 'dell',
    originCity: get(shipperDetails, 'ShipCity', ''),
    originState: get(shipperDetails, 'FK_ShipState', ''),
    originZip: get(shipperDetails, 'ShipZip', ''),
    originCountryCode: get(shipperDetails, 'FK_ShipCountry', ''),
    destCity: get(consigneeDetails, 'ConCity', ''),
    destState: get(consigneeDetails, 'FK_ConState', ''),
    destZip: get(consigneeDetails, 'ConZip', ''),
    destCountryCode: get(consigneeDetails, 'FK_ConCountry', ''),
    eventCity: get(consigneeDetails, 'ConCity', 'Unknown'),
    eventState: get(consigneeDetails, 'FK_ConState', 'Unknown'),
    eventZip: get(consigneeDetails, 'ConZip', 'Unknown'),
    eventCountryCode: get(consigneeDetails, 'FK_ConCountry', 'Unknown'),
    vpod: await getPresignedUrl({ housebill: houseBill }),
  };
  return payload;
}

async function getDocFromWebsli({ housebill }) {
  try {
    const url = `${process.env.GET_DOCUMENT_API}/${process.env.WEBSLI_KEY}/housebill=${housebill}/doctype=HCPOD`;
    const queryType = await axios.get(url);
    const { filename, b64str } = get(queryType, 'data.wtDocs.wtDoc[0]', {});
    return { filename, b64str };
  } catch (error) {
    console.error('error while calling websli endpoint: ', get(error, 'response.data'));
    throw error;
  }
}

async function getPresignedUrl({ housebill }) {
  const { filename, b64str } = await getDocFromWebsli({ housebill });
  const { Key } = await createS3File({ filename, body: Buffer.from(b64str, 'base64') });
  return await generatePreSignedURL({ filename: Key });
}

async function createS3File({ filename, body }) {
  const params = {
    Key: filename,
    Body: body,
    Bucket: process.env.DOCUMENTS_BUCKET,
    ContentType: 'application/pdf',
  };
  return await S3.upload(params).promise();
}

async function generatePreSignedURL({ filename }) {
  const params = {
    Key: filename,
    Bucket: process.env.DOCUMENTS_BUCKET,
    Expires: 24 * 60,
  };
  return await S3.getSignedUrlPromise('getObject', params);
}

async function processAndDeliverMessage({ payload, customerId }) {
  try {
    const { TopicArn } = await getTopicArn('ShipmentAndMilestone');
    console.info('TopicArn: ', TopicArn);
    const params = {
      Message: JSON.stringify(payload),
      TopicArn,
      MessageAttributes: {
        customer_id: {
          DataType: 'String',
          StringValue: customerId.toString(),
        },
      },
    };
    const response = await sns.publish(params).promise();
    console.info('SNS publish: ', response);
  } catch (error) {
    console.error('SNSPublishError: ', error);
    throw error;
  }
}

async function getTopicArn(snsEventType) {
  try {
    const params = {
      Key: {
        Event_Type: snsEventType,
      },
      TableName: process.env.EVENTING_TOPICS_TABLE,
    };
    const response = await dynamoDB.get(params).promise();
    return {
      TopicArn: response.Item.Full_Payload_Topic_Arn,
    };
  } catch (error) {
    console.error(error);
    throw error;
  }
}

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
