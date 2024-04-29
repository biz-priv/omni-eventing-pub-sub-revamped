'use strict';

const AWS = require('aws-sdk');

const dynamoDB = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS({ apiVersion: '2010-03-31' });
const moment = require('moment-timezone');
const _ = require('lodash');
const { v4: uuidv4 } = require('uuid');
const Joi = require('joi');

const statusMapping = {
  APU: { description: 'PICK UP ATTEMPT', stopSequence: 1 },
  SER: { description: 'SHIPMENT EN ROUTE', stopSequence: 1 },
  COB: { description: 'INTRANSIT', stopSequence: 1 },
  AAG: { description: 'ARRIVED AT DESTINATION GATEWAY', stopSequence: 2 },
  REF: { description: 'SHIPMENT REFUSED', stopSequence: 1 },
  APL: { description: 'ONSITE', stopSequence: 1 },
  WEB: { description: 'NEW WEB SHIPMENT', stopSequence: 1 },
  AAO: { description: 'ARRIVED AT OMNI DESTINATION', stopSequence: 2 },
  AAD: { description: 'ARRIVED AT DESTINATION', stopSequence: 2 },
  SOS: { description: 'EMERGENCY WEATHER DELAY', stopSequence: 1 },
  SDE: { description: 'SHIPMENT DELAYED', stopSequence: 1 },
  DGW: { description: 'SHIPMENT DEPARTED GATEWAY', stopSequence: 1 },
  TTC: { description: 'LOADED', stopSequence: 1 },
  NEW: { description: 'NEW SHIPMENT', stopSequence: 1 },
  SRS: { description: 'SHIPMENT RETURNED TO SHIPPER', stopSequence: 1 },
  TPC: { description: 'TRANSFER TO PARTNER CARRIER', stopSequence: 1 },
  PUP: { description: 'PICKED UP', stopSequence: 1 },
  OFD: { description: 'OUT FOR DELIVERY', stopSequence: 2 },
  CAN: { description: 'CANCELLED', stopSequence: 1 },
  OSD: { description: 'SHIPMENT DAMAGED', stopSequence: 1 },
  RCS: { description: 'RECONSIGNED', stopSequence: 1 },
  ADL: { description: 'DELIVERY ATTEMPTED', stopSequence: 2 },
  LOD: { description: 'LOADED', stopSequence: 1 },
  DEL: { description: 'DELIVERED', stopSequence: 2 },
  ED: { description: 'ESTIMATED DELIVERY', stopSequence: 2 },
  APD: { description: 'DELIVERY APPOINTMENT SCHEDULED', stopSequence: 2 },
  DAR: { description: 'DELIVERY APPOINTMENT REQUESTED', stopSequence: 2 },
  HAW: { description: 'HELD AT WAREHOUSE', stopSequence: 2 },
  DAS: { description: 'DELIVERY APPOINTMENT SECURED', stopSequence: 2 },
};

module.exports.handler = async (event, context) => {
  console.info('event:', JSON.stringify(event));

  try {
    await Promise.all(
      event.Records.map(async (record) => {
        // Check if the event is a REMOVE operation and Ignore it.
        if (record.eventName === 'REMOVE') {
          console.info('Skipping REMOVE event.');
          return;
        }

        const tableName = record.eventSourceARN.split('/')[1];
        console.info(
          '🚀 ~ file: milestone-updates.js:57 ~ event.Records.map ~ tableName:',
          tableName
        );
        const newImage = AWS.DynamoDB.Converter.unmarshall(_.get(record, 'dynamodb.NewImage', {}));
        const oldImage = AWS.DynamoDB.Converter.unmarshall(_.get(record, 'dynamodb.OldImage', {}));

        // Check which table the event originated from
        if (tableName === process.env.SHIPMENT_MILESTONE_TABLE) {
          await processShipmentMilestone(newImage);
        } else if (tableName === process.env.SHIPMENT_HEADER_TABLE) {
          await processShipmentHeader(newImage, oldImage);
        } else {
          console.info(`Unknown table: ${tableName}`);
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
      const OrderNo = _.get(newImage, 'PK_OrderNo');
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
      const shipperDetails = await queryShipperDetails(OrderNo);
      const consigneeDetails = await queryConsigneeDetails(OrderNo);

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
        carrier: _.get(shipperDetails, 'ShipName', ''),
        statusCode: 'DAS',
        lastUpdateDate: scheduledDateTime,
        estimatedDeliveryDate,
        identifier: 'NA',
        statusDescription: 'DELIVERY APPOINTMENT SECURED',
        retailerMoniker: 'dell',
        originCity: _.get(shipperDetails, 'ShipCity', ''),
        originState: _.get(shipperDetails, 'FK_ShipState', ''),
        originZip: _.get(shipperDetails, 'ShipZip', ''),
        originCountryCode: _.get(shipperDetails, 'FK_ShipCountry', ''),
        destCity: _.get(consigneeDetails, 'ConCity', ''),
        destState: _.get(consigneeDetails, 'FK_ConState', ''),
        destZip: _.get(consigneeDetails, 'ConZip', ''),
        destCountryCode: _.get(consigneeDetails, 'FK_ConCountry', ''),
        eventState: _.get(consigneeDetails, 'FK_ConState', ''),
        eventCity: _.get(consigneeDetails, 'ConCity', ''),
        eventZip: _.get(consigneeDetails, 'ConZip', ''),
        eventCountryCode: _.get(consigneeDetails, 'FK_ConCountry', ''),
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
      await saveToDynamoDB(payload, customerId, 'Pending', OrderNo);
      console.info('The record is processed');
    } else {
      console.info('No changes in the ScheduleDateTime field');
    }
  } catch (error) {
    console.error('🚀 ~ file: milestone-updates.js:98 ~ processShipmentHeader ~ error:', error);
    throw error;
  }
}

async function processShipmentMilestone(newImage) {
  const orderNo = _.get(newImage, 'FK_OrderNo', '');
  const statusId = _.get(newImage, 'FK_OrderStatusId', '');
  const eventDateTime = _.get(newImage, 'EventDateTime', '');

  // Check if the orderStatusId is not in the statusMapping object
  if (!_.has(statusMapping, statusId)) {
    console.info(`Skipping execution for orderStatusId: ${statusId}`);
    return;
  }

  // Skip processing if EventDateTime is "1900-01-01 00:00:00.000"
  if (eventDateTime === '1900-01-01 00:00:00.000') {
    console.info('Skipping record with EventDateTime of 1900-01-01 00:00:00.000');
    return;
  }

  // Check if ProcessState is equal to 'Not Processed'
  if (_.get(newImage, 'ProcessState', '') === 'Not Processed') {
    try {
      // Update a column in the same table to set ProcessState as 'Pending'
      await updateProcessState(newImage, 'Pending');
      const payload = await processDynamoDBRecord(newImage);

      // Get all customer IDs based on the tracking number
      const trackingNo = _.get(payload, 'trackingNo');
      const customerIds = await getCustomer(trackingNo);
      await saveToDynamoDB(payload, customerIds.join(), 'Pending', orderNo);
      // Update a column in the same table to set ProcessState as 'Processed'
      await updateProcessState(newImage, 'Processed');
      console.info('The record is processed');
    } catch (error) {
      console.error(`Error : ${error.message}`);
      // Save the error message to SHIPMENT_EVENT_STATUS_TABLE with status "Error"
      await saveToDynamoDB(
        {
          id: uuidv4(),
          FK_OrderNo: orderNo,
          trackingNo: 'No trackingNo',
          InsertedTimeStamp: moment.tz('America/Chicago').format('YYYY:MM:DD HH:mm:ss').toString(),
          deliveryStatus: 'Error',
          errorMessage: error,
        },
        '',
        'Error'
      );
    }
  }
}

async function updateProcessState(newImage, processState) {
  const orderNo = _.get(newImage, 'FK_OrderNo', '');
  const orderStatusId = _.get(newImage, 'FK_OrderStatusId', '');

  if (!orderNo || !orderStatusId) {
    throw new Error('Missing orderNo or orderStatusId in the newImage.');
  }

  const params = {
    TableName: process.env.SHIPMENT_MILESTONE_TABLE,
    Key: {
      FK_OrderNo: orderNo,
      FK_OrderStatusId: orderStatusId,
    },
    UpdateExpression: 'SET ProcessState = :processState',
    ExpressionAttributeValues: {
      ':processState': processState,
    },
  };

  try {
    await dynamoDB.update(params).promise();
    console.info(`Updated the ProcessState to '${processState}'`);
  } catch (error) {
    console.error('Error updating ProcessState:', error.message);
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

async function processDynamoDBRecord(dynamodbRecord) {
  try {
    const {
      FK_OrderNo: OrderNo,
      FK_OrderStatusId: OrderStatusId,
      EventDateTime,
      UUid: id,
    } = dynamodbRecord;

    const headerDetails = await queryHeaderDetails(OrderNo);
    console.info(
      '🚀 ~ file: milestone-updates.js:289 ~ processDynamoDBRecord ~ headerDetails:',
      headerDetails
    );
    const { ETADateTime, Housebill } = headerDetails;
    const estimatedDeliveryDate =
      ETADateTime === 'NULL' || ETADateTime === '' || _.includes(ETADateTime, '1900')
        ? 'NA'
        : ETADateTime;
    const shipperDetails = await queryShipperDetails(OrderNo);
    const consigneeDetails = await queryConsigneeDetails(OrderNo);

    if (
      !OrderNo ||
      !OrderStatusId ||
      !EventDateTime ||
      !Housebill ||
      !shipperDetails ||
      !consigneeDetails
    ) {
      throw new Error('One or more mandatory fields are missing in the payload');
    }

    const stopsequence = statusMapping[OrderStatusId]
      ? statusMapping[OrderStatusId].stopSequence
      : 2;
    const statusInfo = statusMapping[OrderStatusId];

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
      id,
      trackingNo: Housebill,
      carrier: _.get(shipperDetails, 'ShipName', ''),
      statusCode: OrderStatusId,
      lastUpdateDate: EventDateTime,
      estimatedDeliveryDate,
      identifier: 'NA',
      statusDescription: _.get(statusInfo, 'description'),
      retailerMoniker: 'dell',
      originCity: _.get(shipperDetails, 'ShipCity', ''),
      originState: _.get(shipperDetails, 'FK_ShipState', ''),
      originZip: _.get(shipperDetails, 'ShipZip', ''),
      originCountryCode: _.get(shipperDetails, 'FK_ShipCountry', ''),
      destCity: _.get(consigneeDetails, 'ConCity', ''),
      destState: _.get(consigneeDetails, 'FK_ConState', ''),
      destZip: _.get(consigneeDetails, 'ConZip', ''),
      destCountryCode: _.get(consigneeDetails, 'FK_ConCountry', ''),
    };

    if (stopsequence === 1) {
      payload.eventCity = _.get(shipperDetails, 'ShipCity', 'Unknown');
      payload.eventState = _.get(shipperDetails, 'FK_ShipState', 'Unknown');
      payload.eventZip = _.get(shipperDetails, 'ShipZip', 'Unknown');
      payload.eventCountryCode = _.get(shipperDetails, 'FK_ShipCountry', 'Unknown');
    } else if (stopsequence === 2) {
      payload.eventCity = _.get(consigneeDetails, 'ConCity', 'Unknown');
      payload.eventState = _.get(consigneeDetails, 'FK_ConState', 'Unknown');
      payload.eventZip = _.get(consigneeDetails, 'ConZip', 'Unknown');
      payload.eventCountryCode = _.get(consigneeDetails, 'FK_ConCountry', 'Unknown');
    }

    // Validate the payload against the schema
    const validationResult = schema.validate(payload, { abortEarly: false });

    if (_.get(validationResult, 'error')) {
      // Joi validation failed, throw an error with the details
      throw new Error(`Payload validation error: ${_.get(validationResult, 'error.message')}`);
    }
    return payload;
  } catch (error) {
    console.error('Error processing DynamoDB record:', error);
    throw error;
  }
}

// eslint-disable-next-line consistent-return
async function queryShipperDetails(OrderNo) {
  const params = {
    TableName: process.env.SHIPPER_TABLE,
    KeyConditionExpression: 'FK_ShipOrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': OrderNo,
    },
  };
  console.info('🚀 ~ file: milestone-updates.js:390 ~ queryShipperDetails ~ params:', params);
  try {
    const result = await dynamoDB.query(params).promise();
    if (_.get(result, 'Items', []).length > 0) {
      return result.Items[0];
    }
    throw new Error(`No shipper details found for FK_ShipOrderNo: ${OrderNo}`);
  } catch (error) {
    console.error('Error querying shipper details:', error.message);
  }
}

// eslint-disable-next-line consistent-return
async function queryConsigneeDetails(OrderNo) {
  const params = {
    TableName: process.env.CONSIGNEE_TABLE,
    KeyConditionExpression: 'FK_ConOrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': OrderNo,
    },
  };
  console.info('🚀 ~ file: milestone-updates.js:409 ~ queryConsigneeDetails ~ params:', params);
  try {
    const result = await dynamoDB.query(params).promise();
    if (_.get(result, 'Items', []).length > 0) {
      return result.Items[0];
    }
    throw new Error(`No consignee details found for FK_ConOrderNo: ${OrderNo}`);
  } catch (error) {
    console.error('Error querying consignee details:', error.message);
  }
}

// eslint-disable-next-line consistent-return
async function queryHeaderDetails(OrderNo) {
  const params = {
    TableName: process.env.SHIPMENT_HEADER_TABLE,
    KeyConditionExpression: 'PK_OrderNo = :orderNo',
    ExpressionAttributeValues: {
      ':orderNo': OrderNo,
    },
  };
  console.info('🚀 ~ file: milestone-updates.js:428 ~ queryHeaderDetails ~ params:', params);
  try {
    const result = await dynamoDB.query(params).promise();
    if (_.get(result, 'Items', []).length > 0) {
      return result.Items[0];
    }
    throw new Error(`No header details found for ${OrderNo}`);
  } catch (error) {
    console.error('Error querying header details:', error.message);
  }
}

async function getCustomer(housebill) {
  const params = {
    TableName: process.env.CUSTOMER_ENTITLEMENT_TABLE,
    IndexName: process.env.ENTITLEMENT_HOUSEBILL_INDEX,
    KeyConditionExpression: 'HouseBillNumber = :Housebill',
    ExpressionAttributeValues: {
      ':Housebill': housebill,
    },
  };

  try {
    const data = await dynamoDB.query(params).promise();
    if (_.get(data, 'Items', []).length > 0) {
      // Extract an array of customer IDs from the DynamoDB objects
      const customerIds = data.Items.map((item) => item.CustomerID);
      return customerIds;
    }
    throw new Error(`No CustomerID found for this ${housebill} in entitlements table`);
  } catch (error) {
    console.error('Validation error:', error);
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
