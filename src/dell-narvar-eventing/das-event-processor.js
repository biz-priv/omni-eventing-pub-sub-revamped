'use strict';

const AWS = require('aws-sdk');

const dynamoDB = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS({ apiVersion: '2010-03-31' });
const moment = require('moment-timezone');
const _ = require('lodash');
// const { v4: uuidv4 } = require('uuid');
const Joi = require('joi');

module.exports.handler = async (event, context) => {
  console.info('event:', JSON.stringify(event));

  try {
    await Promise.all(
      event.Records.map(async (record) => {
        if (record.eventName === 'REMOVE') {
          console.info('Skipping REMOVE event.');
          return;
        }

        const tableName = record.eventSourceARN.split('/')[1];
        console.info('Table Name:', tableName);

        const newImage = AWS.DynamoDB.Converter.unmarshall(_.get(record, 'dynamodb.NewImage', {}));
        const oldImage = AWS.DynamoDB.Converter.unmarshall(_.get(record, 'dynamodb.OldImage', {}));
        console.info(
          '🚀 ~ file: das-event-processor.js:28 ~ event.Records.map ~ oldImage:',
          oldImage
        );

        if (tableName === process.env.SHIPMENT_HEADER_TABLE) {
          await processShipmentHeader(newImage, oldImage);
        } else if (
          tableName === process.env.CONSIGNEE_TABLE ||
          tableName === process.env.SHIPPER_TABLE
        ) {
          await processShipperAndConsignee(newImage, tableName);
        } else if (tableName === process.env.STATUS_TABLE) {
          await processStatusTable(newImage);
        }
      })
    );
  } catch (error) {
    const errorMessage = `An error occurred in function ${context.functionName}. Error details: ${error}.`;
    console.error(errorMessage);

    try {
      await publishToSNS(errorMessage, context.functionName);
    } catch (snsError) {
      console.error('Error publishing to SNS:', snsError);
    }
  }
};

async function processShipmentHeader(newImage, oldImage) {
  try {
    if (
      (newImage.ScheduledDateTime && !oldImage.ScheduledDateTime) ||
      oldImage.ScheduledDateTime !== newImage.ScheduledDateTime
    ) {
      const orderNo = _.get(newImage, 'PK_OrderNo');
      const scheduledDateTime = _.get(newImage, 'ScheduledDateTime', '');
      const housebill = _.get(newImage, 'Housebill');
      console.info(
        '🚀 ~ file: das-event-processor.js:66 ~ processShipmentHeader ~ housebill:',
        housebill
      );
      if (housebill === 0 || housebill === '0') {
        console.info('skipping the process as the housebill number is invalid');
        return;
      }
      console.info(
        '🚀 ~ file: milestone-updates.js:90 ~ processShipmentHeader ~ scheduledDateTime:',
        scheduledDateTime
      );
      if (
        _.includes(scheduledDateTime, '1900') ||
        scheduledDateTime === 'NULL' ||
        scheduledDateTime === ''
      ) {
        console.info('Skipping execution for scheduledDateTime: ', scheduledDateTime);
        return;
      }
      const etaDateTime = _.get(newImage, 'ETADateTime');
      const estimatedDeliveryDate =
        etaDateTime === 'NULL' || etaDateTime === '' || _.includes(etaDateTime, '1900')
          ? 'NA'
          : etaDateTime;
      const shipperDetails = await queryShipperDetails(orderNo);
      console.info(
        '🚀 ~ file: das-event-processor.js:83 ~ processShipmentHeader ~ shipperDetails:',
        shipperDetails
      );
      const consigneeDetails = await queryConsigneeDetails(orderNo);
      console.info(
        '🚀 ~ file: das-event-processor.js:85 ~ processShipmentHeader ~ consigneeDetails:',
        consigneeDetails
      );

      if (
        shipperDetails.length > 0 &&
        shipperDetails.every((item) => {
          const {
            // eslint-disable-next-line camelcase
            ShipName,
            ShipZip,
            ShipCity,
            // eslint-disable-next-line camelcase
            FK_ShipState,
            // eslint-disable-next-line camelcase
            FK_ShipCountry,
          } = item;

          return _.every(
            [
              ShipName,
              ShipZip,
              ShipCity,
              // eslint-disable-next-line camelcase
              FK_ShipCountry,
              // eslint-disable-next-line camelcase
              FK_ShipState,
            ],
            (value) => !_.isEmpty(value) && value !== 'NULL'
          );
        })
      ) {
        console.info('details are present.');
        // Insert order number and status columns into status table
        newImage.ShipperStatus = 'READY';
      } else {
        newImage.ShipName = _.get(shipperDetails, '[0].ShipName');
        newImage.FK_ShipCountry = _.get(shipperDetails, '[0].FK_ShipCountry');
        newImage.ShipCity = _.get(shipperDetails, '[0].ShipCity');
        newImage.ShipZip = _.get(shipperDetails, '[0].ShipZip');
        newImage.FK_ShipState = _.get(shipperDetails, '[0].FK_ShipState');
        newImage.ShipperStatus = 'PENDING';
      }
      if (
        consigneeDetails.length > 0 &&
        consigneeDetails.every((item) => {
          const {
            ConZip,
            ConCity,
            // eslint-disable-next-line camelcase
            FK_ConState,
            // eslint-disable-next-line camelcase
            FK_ConCountry,
          } = item;

          return _.every(
            [
              ConZip,
              ConCity,
              // eslint-disable-next-line camelcase
              FK_ConState,
              // eslint-disable-next-line camelcase
              FK_ConCountry,
            ],
            (value) => !_.isEmpty(value) && value !== 'NULL'
          );
        })
      ) {
        newImage.ConsigneeStatus = 'READY';
      } else {
        newImage.ConsigneeStatus = 'PENDING';
        newImage.FK_ConCountry = _.get(consigneeDetails, '[0].FK_ConCountry');
        newImage.ConCity = _.get(consigneeDetails, '[0].ConCity');
        newImage.ConZip = _.get(consigneeDetails, '[0].ConZip');
        newImage.FK_ConState = _.get(consigneeDetails, '[0].FK_ConState');
      }
      if (newImage.ConsigneeStatus === 'PENDING' || newImage.ShipperStatus === 'PENDING') {
        await insertShipmentHeaderIntoStatusTable(newImage);
        // Stop processing further
        console.info('No shipper or consignee data found. Stopping further processing.');
        return;
      }
      // Validate the payload using Joi
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
      });

      const payload = {
        id: _.get(newImage, 'UUid'), // UUid from shipment header dynamodb table
        trackingNo: housebill,
        carrier: _.get(shipperDetails, '[0]ShipName', ''),
        statusCode: 'DAS',
        lastUpdateDate: scheduledDateTime,
        estimatedDeliveryDate,
        identifier: 'NA',
        statusDescription: 'DELIVERY APPOINTMENT SECURED',
        retailerMoniker: 'dell',
        originCity: _.get(shipperDetails, '[0]ShipCity', ''),
        originState: _.get(shipperDetails, '[0]FK_ShipState', ''),
        originZip: _.get(shipperDetails, '[0]ShipZip', ''),
        originCountryCode: _.get(shipperDetails, '[0]FK_ShipCountry', ''),
        destCity: _.get(consigneeDetails, '[0]ConCity', ''),
        destState: _.get(consigneeDetails, '[0]FK_ConState', ''),
        destZip: _.get(consigneeDetails, '[0]ConZip', ''),
        destCountryCode: _.get(consigneeDetails, '[0]FK_ConCountry', ''),
        eventState: _.get(consigneeDetails, '[0]FK_ConState', ''),
        eventCity: _.get(consigneeDetails, '[0]ConCity', ''),
        eventZip: _.get(consigneeDetails, '[0]ConZip', ''),
        eventCountryCode: _.get(consigneeDetails, '[0]FK_ConCountry', ''),
      };

      console.info(
        '🚀 ~ file: milestone-updates.js:99 ~ processShipmentHeader ~ payload:',
        payload
      );
      const validationResult = schema.validate(payload, { abortEarly: false });

      if (_.get(validationResult, 'error')) {
        // Joi validation failed, construct a more informative error message
        const errorDetails = validationResult.error.details
          .map((detail) => `"${detail.context.key}" ${detail.message}`)
          .join('\n');
        throw new Error(
          `\nPayload validation error for Housebill: ${_.get(payload, 'trackingNo')} :\n${errorDetails}.`
        );
      }
      const billNo = Number(_.get(newImage, 'BillNo'));
      console.info('🚀 ~ file: milestone-updates.js:170 ~ processShipmentHeader ~ billNo:', billNo);
      const customerId = mapBillNoToCustomerId(billNo, process.env.STAGE);
      console.info(
        '🚀 ~ file: milestone-updates.js:167 ~ processShipmentHeader ~ customerIds:',
        customerId
      );
      if (!customerId) {
        console.info('Customer ID not found for billNo:', billNo);
        await deleteDynamoRecord({ orderNo });
        return;
      }
      await saveToDynamoDB(payload, customerId, 'Pending', orderNo);
      await updateStatus(process.env.STATUS_TABLE, orderNo, 'SENT');
      console.info('The record is processed');
    } else {
      console.info('No changes in the ScheduleDateTime field');
      return;
    }
  } catch (error) {
    console.error('🚀 ~ file: milestone-updates.js:98 ~ processShipmentHeader ~ error:', error);
    throw error;
  }
}

async function publishToSNS(message, subject) {
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

async function checkRecordExistsInStatusTable(orderNo) {
  const params = {
    TableName: process.env.STATUS_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };

  try {
    const data = await dynamoDB.query(params).promise();
    return _.get(data, 'Items', []);
  } catch (error) {
    console.error('Error checking record existence:', error);
    throw error;
  }
}

async function updateStatus(tableName, orderNo, statusValue) {
  const params = {
    TableName: tableName,
    Key: {
      FK_OrderNo: orderNo,
    },
    UpdateExpression: 'SET #Status = :statusValue',
    ExpressionAttributeNames: { '#Status': 'Status' },
    ExpressionAttributeValues: {
      ':statusValue': statusValue,
    },
  };

  try {
    await dynamoDB.update(params).promise();
  } catch (error) {
    console.error('Error updating status:', error);
    throw error;
  }
}

async function checkAllStatusReady(orderNo) {
  const statusParams = {
    TableName: process.env.STATUS_TABLE,
    KeyConditionExpression: 'FK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };

  try {
    const data = await dynamoDB.query(statusParams).promise();
    if (data.Items.length === 0) {
      return false;
    }
    const shipmentHeaderStatus = _.get(data, 'Items[0].ShipmentHeaderStatus', '');
    const shipperStatus = _.get(data, 'Items[0].ShipperStatus', '');
    const consigneeStatus = _.get(data, 'Items[0].ConsigneeStatus', '');
    return (
      shipmentHeaderStatus === 'READY' && shipperStatus === 'READY' && consigneeStatus === 'READY'
    );
  } catch (error) {
    console.error('Error checking all statuses:', error);
    throw error;
  }
}

async function saveToDynamoDB(payload, customerId, deliveryStatus, orderNo) {
  const params = {
    TableName: process.env.SHIPMENT_EVENT_STATUS_TABLE,
    Item: {
      id: payload.id,
      FK_OrderNo: orderNo,
      trackingNo: payload.trackingNo,
      customerId: String(customerId),
      InsertedTimeStamp: moment.tz('America/Chicago').format('YYYY:MM:DD HH:mm:ss').toString(),
      payload: JSON.stringify(payload),
      deliveryStatus,
    },
  };
  try {
    await dynamoDB.put(params).promise();
  } catch (error) {
    console.error('Error saving to DynamoDB:', error);
  }
}

// Define your data as an array of objects
const data = {
  dev: [
    { customerId: 10465268, billNo: 53370 },
    { customerId: 10467696, billNo: 53385 },
    { customerId: 10489251, billNo: 53376 },
    { customerId: 10501014, billNo: 53345 },
    { customerId: 10493743, billNo: 53525 },
    { customerId: 10508494, billNo: 53355 },
    { customerId: 10516460, billNo: 53517 },
    { customerId: 10528691, billNo: 53367 },
    { customerId: 10490821, billNo: 53353 },
    { customerId: 10514167, billNo: 53518 },
    { customerId: 10495727, billNo: 53551 },
    { customerId: 10510168, billNo: 53529 },
    { customerId: 10477361, billNo: 53526 },
    { customerId: 10486375, billNo: 53527 },
    { customerId: 10512447, billNo: 53523 },
    { customerId: 10472786, billNo: 53342 },
    { customerId: 10513992, billNo: 53510 },
    { customerId: 10489488, billNo: 53373 },
    { customerId: 10483834, billNo: 53341 },
    { customerId: 10508470, billNo: 53346 },
    { customerId: 10498369, billNo: 53389 },
    { customerId: 10480788, billNo: 53524 },
    { customerId: 10505959, billNo: 53350 },
    { customerId: 10537523, billNo: 54302 },
    { customerId: 10511581, billNo: 53386 },
    { customerId: 10488327, billNo: 53344 },
    { customerId: 10488510, billNo: 53516 },
    { customerId: 10515995, billNo: 53354 },
    { customerId: 10478419, billNo: 53536 },
    { customerId: 10467083, billNo: 53347 },
    { customerId: 10484491, billNo: 53374 },
    { customerId: 10508344, billNo: 53379 },
    { customerId: 10492416, billNo: 53384 },
    { customerId: 10483049, billNo: 53395 },
    { customerId: 10513583, billNo: 53362 },
    { customerId: 10502508, billNo: 53356 },
    { customerId: 10498497, billNo: 53357 },
    { customerId: 10514179, billNo: 53343 },
    { customerId: 10478676, billNo: 53392 },
    { customerId: 10467792, billNo: 53360 },
    { customerId: 10498854, billNo: 53364 },
    { customerId: 10521382, billNo: 53393 },
    { customerId: 10489429, billNo: 53514 },
    { customerId: 10491834, billNo: 53383 },
    { customerId: 10526650, billNo: 53359 },
    { customerId: 10498380, billNo: 53378 },
    { customerId: 10524886, billNo: 53522 },
    { customerId: 10500974, billNo: 53371 },
    { customerId: 10469089, billNo: 53352 },
    { customerId: 10501000, billNo: 53368 },
    { customerId: 10473932, billNo: 53512 },
    { customerId: 10471467, billNo: 53387 },
    { customerId: 10467672, billNo: 53511 },
    { customerId: 10496344, billNo: 53349 },
    { customerId: 10494629, billNo: 53361 },
    { customerId: 10477581, billNo: 53508 },
    { customerId: 10483790, billNo: 53372 },
    { customerId: 10454715, billNo: 11935 },
    { customerId: 10472932, billNo: 53513 },
    { customerId: 10475501, billNo: 53377 },
    { customerId: 10468791, billNo: 53515 },
    { customerId: 10526135, billNo: 53365 },
    { customerId: 10497338, billNo: 53351 },
    { customerId: 10479472, billNo: 53394 },
    { customerId: 10509038, billNo: 53519 },
    { customerId: 10515481, billNo: 53375 },
    { customerId: 10460067, billNo: 53366 },
    { customerId: 10502342, billNo: 53391 },
    { customerId: 10522143, billNo: 53348 },
    { customerId: 10475517, billNo: 53358 },
    { customerId: 10511440, billNo: 53388 },
    { customerId: 10483662, billNo: 53369 },
    { customerId: 10528776, billNo: 53521 },
    { customerId: 10476923, billNo: 53509 },
    { customerId: 10516026, billNo: 53390 },
    { customerId: 10539222, billNo: 54304 },
    { customerId: 10536728, billNo: 54303 },
    { customerId: 10503834, billNo: 53363 },
  ],
  prod: [
    { customerId: 10583560, billNo: 53368 },
    { customerId: 10585356, billNo: 53356 },
    { customerId: 10582182, billNo: 53391 },
    { customerId: 10582083, billNo: 53343 },
    { customerId: 10589900, billNo: 53517 },
    { customerId: 10587112, billNo: 53510 },
    { customerId: 10029364, billNo: 11935 },
    { customerId: 10582086, billNo: 53364 },
    { customerId: 10585023, billNo: 53348 },
    { customerId: 10584378, billNo: 53351 },
    { customerId: 10588333, billNo: 53377 },
    { customerId: 10585388, billNo: 53378 },
    { customerId: 10582214, billNo: 53393 },
    { customerId: 10582507, billNo: 53374 },
    { customerId: 10585671, billNo: 53350 },
    { customerId: 10592397, billNo: 53508 },
    { customerId: 10589932, billNo: 53512 },
    { customerId: 10582051, billNo: 53366 },
    { customerId: 10586729, billNo: 53395 },
    { customerId: 10585575, billNo: 53344 },
    { customerId: 10587176, billNo: 53521 },
    { customerId: 10584442, billNo: 53341 },
    { customerId: 10584705, billNo: 53357 },
    { customerId: 10589957, billNo: 53353 },
    { customerId: 10589703, billNo: 53527 },
    { customerId: 10584143, billNo: 53362 },
    { customerId: 10582179, billNo: 53376 },
    { customerId: 10582571, billNo: 53347 },
    { customerId: 10585838, billNo: 53369 },
    { customerId: 10588591, billNo: 53525 },
    { customerId: 10590126, billNo: 53519 },
    { customerId: 10587172, billNo: 53513 },
    { customerId: 10584513, billNo: 53352 },
    { customerId: 10583872, billNo: 53384 },
    { customerId: 10584410, billNo: 53383 },
    { customerId: 10584673, billNo: 53389 },
    { customerId: 10589925, billNo: 53361 },
    { customerId: 10587056, billNo: 53360 },
    { customerId: 10586674, billNo: 53342 },
    { customerId: 10580886, billNo: 53345 },
    { customerId: 10585206, billNo: 53522 },
    { customerId: 10604886, billNo: 54304 },
    { customerId: 10587120, billNo: 53388 },
    { customerId: 10589077, billNo: 53514 },
    { customerId: 10612211, billNo: 54302 },
    { customerId: 10588724, billNo: 53370 },
    { customerId: 10590519, billNo: 53515 },
    { customerId: 10592595, billNo: 53536 },
    { customerId: 10585943, billNo: 53365 },
    { customerId: 10588376, billNo: 53511 },
    { customerId: 10585934, billNo: 53372 },
    { customerId: 10588344, billNo: 53529 },
    { customerId: 10590583, billNo: 53518 },
    { customerId: 10608120, billNo: 54303 },
    { customerId: 10586960, billNo: 53394 },
    { customerId: 10582603, billNo: 53387 },
    { customerId: 10585870, billNo: 53371 },
    { customerId: 10589391, billNo: 53551 },
    { customerId: 10588628, billNo: 53392 },
    { customerId: 10587088, billNo: 53373 },
    { customerId: 10588179, billNo: 53367 },
    { customerId: 10593140, billNo: 53524 },
    { customerId: 10584120, billNo: 53379 },
    { customerId: 10583515, billNo: 53354 },
    { customerId: 10584218, billNo: 53390 },
    { customerId: 10587163, billNo: 53509 },
    { customerId: 10581789, billNo: 53358 },
    { customerId: 10584282, billNo: 53359 },
    { customerId: 10584152, billNo: 53349 },
    { customerId: 10584250, billNo: 53363 },
    { customerId: 10586992, billNo: 53385 },
    { customerId: 10585902, billNo: 53355 },
    { customerId: 10596785, billNo: 53526 },
    { customerId: 10580950, billNo: 53346 },
    { customerId: 10588639, billNo: 53523 },
    { customerId: 10581821, billNo: 53386 },
    { customerId: 10582137, billNo: 53375 },
    { customerId: 10589054, billNo: 53516 },
  ],
};

// Create a mapping function
function mapBillNoToCustomerId(billNo, stage) {
  // Get the data based on the provided stage
  const stageData = _.get(data, stage, []);

  // Convert the stage data into a Map where billNo is the key and customerId is the value
  const mapping = new Map(
    stageData.map((entry) => [_.get(entry, 'billNo'), _.get(entry, 'customerId')])
  );

  // Retrieve the customerId corresponding to the billNo from the mapping
  return mapping.get(billNo);
}

async function queryShipperDetails(orderNo) {
  const params = {
    TableName: process.env.SHIPPER_TABLE,
    KeyConditionExpression: 'FK_ShipOrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };
  console.info('🚀 ~ file: milestone-updates.js:390 ~ queryShipperDetails ~ params:', params);
  try {
    const result = await dynamoDB.query(params).promise();

    return _.get(result, 'Items', []);
  } catch (error) {
    console.error('Error querying shipper details:', error.message);
    throw error;
  }
}

async function queryConsigneeDetails(orderNo) {
  const params = {
    TableName: process.env.CONSIGNEE_TABLE,
    KeyConditionExpression: 'FK_ConOrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': orderNo,
    },
  };
  console.info('🚀 ~ file: milestone-updates.js:409 ~ queryConsigneeDetails ~ params:', params);
  try {
    const result = await dynamoDB.query(params).promise();

    return _.get(result, 'Items', []);
  } catch (error) {
    console.error('Error querying consignee details:', error.message);
    throw error;
  }
}

async function processShipperAndConsignee(newImage, tableName) {
  try {
    let orderNo;
    if (tableName === process.env.SHIPPER_TABLE) {
      orderNo = newImage.FK_ShipOrderNo;
    } else {
      orderNo = newImage.FK_ConOrderNo;
    }

    // Query status table if the order is present there or not
    const orderNoExists = await checkRecordExistsInStatusTable(orderNo);

    if (orderNoExists.length > 0 && !(_.get(orderNoExists, '[0]Status') === 'SENT')) {
      if (tableName === process.env.SHIPPER_TABLE) {
        await updateShipperStatus(newImage);
      } else {
        await updateConsigneeStatus(newImage);
      }
      // Check if all statuses are READY and update main status column
      const allReady = await checkAllStatusReady(orderNo);
      if (allReady) {
        await updateStatus(process.env.STATUS_TABLE, orderNo, 'READY');
      }
    }
  } catch (error) {
    console.error('Error processing shipper and consignee:', error);
    throw error;
  }
}

async function processStatusTable(newImage) {
  const orderNo = _.get(newImage, 'FK_OrderNo');
  try {
    const scheduledDateTime = _.get(newImage, 'ScheduledDateTime', '');
    console.info(
      '🚀 ~ file: milestone-updates.js:90 ~ processShipmentHeader ~ scheduledDateTime:',
      scheduledDateTime
    );
    if (
      _.includes(scheduledDateTime, '1900') ||
      scheduledDateTime === 'NULL' ||
      scheduledDateTime === ''
    ) {
      console.info('Skipping execution for scheduledDateTime: ', scheduledDateTime);
      return;
    }
    const etaDateTime = _.get(newImage, 'ETADateTime');
    const estimatedDeliveryDate =
      etaDateTime === 'NULL' || etaDateTime === '' || _.includes(etaDateTime, '1900')
        ? 'NA'
        : etaDateTime;
    // Validate the payload using Joi
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
    });

    const payload = {
      id: _.get(newImage, 'UUid'), // UUid from shipment header dynamodb table
      trackingNo: _.get(newImage, 'Housebill'),
      carrier: _.get(newImage, 'ShipName', ''),
      statusCode: 'DAS',
      lastUpdateDate: scheduledDateTime,
      estimatedDeliveryDate,
      identifier: 'NA',
      statusDescription: 'DELIVERY APPOINTMENT SECURED',
      retailerMoniker: 'dell',
      originCity: _.get(newImage, 'ShipCity', ''),
      originState: _.get(newImage, 'FK_ShipState', ''),
      originZip: _.get(newImage, 'ShipZip', ''),
      originCountryCode: _.get(newImage, 'FK_ShipCountry', ''),
      destCity: _.get(newImage, 'ConCity', ''),
      destState: _.get(newImage, 'FK_ConState', ''),
      destZip: _.get(newImage, 'ConZip', ''),
      destCountryCode: _.get(newImage, 'FK_ConCountry', ''),
      eventState: _.get(newImage, 'FK_ConState', ''),
      eventCity: _.get(newImage, 'ConCity', ''),
      eventZip: _.get(newImage, 'ConZip', ''),
      eventCountryCode: _.get(newImage, 'FK_ConCountry', ''),
    };
    console.info('🚀 ~ file: milestone-updates.js:99 ~ processShipmentHeader ~ payload:', payload);
    const validationResult = schema.validate(payload, { abortEarly: false });

    if (_.get(validationResult, 'error')) {
      // Joi validation failed, construct a more informative error message
      const errorDetails = validationResult.error.details
        .map((detail) => `"${detail.context.key}" ${detail.message}`)
        .join('\n');
      throw new Error(
        `\nPayload validation error for Housebill: ${_.get(payload, 'trackingNo')} :\n${errorDetails}.`
      );
    }
    const billNo = Number(_.get(newImage, 'BillNo'));
    console.info('🚀 ~ file: milestone-updates.js:170 ~ processShipmentHeader ~ billNo:', billNo);
    const customerId = mapBillNoToCustomerId(billNo, process.env.STAGE);
    console.info(
      '🚀 ~ file: milestone-updates.js:167 ~ processShipmentHeader ~ customerIds:',
      customerId
    );
    if (!customerId) {
      console.info('Customer ID not found for billNo:', billNo);
      await deleteDynamoRecord({ orderNo });
      return;
    }
    await saveToDynamoDB(payload, customerId, 'Pending', orderNo);
    await updateStatus(process.env.STATUS_TABLE, orderNo, 'SENT');
    console.info('The record is processed');
  } catch (error) {
    console.error('🚀 ~ file: das-event-processor.js:638 ~ processStatusTable ~ error:', error);
    await updateStatus(process.env.STATUS_TABLE, orderNo, 'FAILED');
    throw error;
  }
}

async function insertShipmentHeaderIntoStatusTable(newImage) {
  try {
    const item = {
      FK_OrderNo: newImage.PK_OrderNo,
      ShipmentHeaderStatus: 'READY',
      ShipperStatus: newImage.ShipperStatus,
      ConsigneeStatus: newImage.ConsigneeStatus,
      Status: 'PENDING',
      ScheduledDateTime: newImage.ScheduledDateTime,
      ETADateTime: newImage.ETADateTime,
      UUid: newImage.UUid,
      Housebill: newImage.Housebill,
      BillNo: newImage.BillNo,
      FK_ShipCountry: _.get(newImage, 'FK_ShipCountry', ''),
      ShipZip: _.get(newImage, 'ShipZip', ''),
      ShipCity: _.get(newImage, 'ShipCity', ''),
      FK_ShipState: _.get(newImage, 'FK_ShipState', ''),
      ShipName: _.get(newImage, 'ShipName', ''),
      FK_ConCountry: _.get(newImage, 'FK_ConCountry', ''),
      ConZip: _.get(newImage, 'ConZip', ''),
      ConCity: _.get(newImage, 'ConCity', ''),
      FK_ConState: _.get(newImage, 'FK_ConState', ''),
    };

    const params = {
      TableName: process.env.STATUS_TABLE,
      Item: item,
    };

    await dynamoDB.put(params).promise();
    console.info('Shipment header inserted into status table:', item);
  } catch (error) {
    console.error('Error inserting shipment header into status table:', error);
    throw error;
  }
}

async function updateShipperStatus(newImage) {
  try {
    const params = {
      TableName: process.env.STATUS_TABLE,
      Key: {
        FK_OrderNo: newImage.FK_ShipOrderNo,
      },
      UpdateExpression:
        'SET ShipperStatus = :status, FK_ShipCountry = :country, ShipZip = :zip, ShipCity = :city, FK_ShipState = :state, ShipName = :name',
      ExpressionAttributeValues: {
        ':status': 'READY',
        ':country': newImage.FK_ShipCountry,
        ':zip': newImage.ShipZip,
        ':city': newImage.ShipCity,
        ':state': newImage.FK_ShipState,
        ':name': newImage.ShipName,
      },
      ReturnValues: 'ALL_NEW',
    };
    console.info('🚀 ~ file: das-event-processor.js:764 ~ updateShipperStatus ~ params:', params);

    const result = await dynamoDB.update(params).promise();
    console.info('Shipper status updated in status table:', result.Attributes);
  } catch (error) {
    console.error('Error updating shipper status in status table:', error);
    throw error;
  }
}

async function updateConsigneeStatus(newImage) {
  try {
    const params = {
      TableName: process.env.STATUS_TABLE,
      Key: {
        FK_OrderNo: newImage.FK_ConOrderNo,
      },
      UpdateExpression:
        'SET ConsigneeStatus = :status, FK_ConCountry = :country, ConZip = :zip, ConCity = :city, FK_ConState = :state',
      ExpressionAttributeValues: {
        ':status': 'READY',
        ':country': newImage.FK_ConCountry,
        ':zip': newImage.ConZip,
        ':city': newImage.ConCity,
        ':state': newImage.FK_ConState,
      },
      ReturnValues: 'ALL_NEW',
    };

    const result = await dynamoDB.update(params).promise();
    console.info('Consignee status updated in status table:', result.Attributes);
  } catch (error) {
    console.error('Error updating consignee status in status table:', error);
    throw error;
  }
}

async function deleteDynamoRecord({ orderNo }) {
  const params = {
    TableName: process.env.STATUS_TABLE,
    Key: { FK_OrderNo: orderNo },
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
