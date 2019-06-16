"use strict";

// Packages
var fs = require("fs");
var avro = require("avsc");
var kafka = require("node-rdkafka");
var oada = require("@oada/oada-cache").default;

// Logging
var logInfo = require("debug")("isoblue-data-uservice:info");
var logError = require("debug")("isoblue-data-uservice:error");

// Parameters
const topics = ["remote", "debug"]; // Kafka topics
const kafka_group_id = process.env.KAFKA_GROUP_ID || "isoblue-gps"; // Kafka group ID
const domain = process.env.OADA_URL; // OADA server URL
const token = process.env.OADA_TOKEN || "abc"; // OADA token
const kafka_broker = process.env.KAFKA_BROKER;

// avro
const type_gps = avro.Type.forSchema(
  JSON.parse(fs.readFileSync("./schema/gps.avsc")),
);
const type_hb = avro.Type.forSchema(
  JSON.parse(fs.readFileSync("./schema/d_hb.avsc")),
);

var tree = {
  bookmarks: {
    _type: "application/vnd.oada.bookmarks.1+json",
    _rev: 0,
    isoblue: {
      _type: "application/vnd.oada.isoblue.1+json",
      _rev: 0,
      "device-index": {
        "*": {
          _type: "application/vnd.oada.isoblue.device.1+json",
          _rev: 0,
          "*": {
            _type: "application/vnd.oada.isoblue.dataset.1+json",
            _rev: 0,
            "day-index": {
              "*": {
                _type: "application/vnd.oada.isoblue.day.1+json",
                _rev: 0,
                "hour-index": {
                  "*": {
                    _type: "application/vnd.oada.isoblue.hour.1+json",
                  },
                },
              },
            },
          },
        },
      },
    },
  },
};

var connectionArgs = {
  websocket: false,
  domain,
  token,
  cache: false,
};

// Process GPS message
function handleGPSmsg(conn, m) {
  const key_split = m.key.toString().split(":");
  const isoblueId = key_split[1]; // ISOBlue ID
  var gps_datum;
  try {
    gps_datum = type_gps.fromBuffer(m.value); // deserialize
  } catch (err) {
    logError("Failed to deserialize a GPS message.");
    return;
  }
  if (gps_datum.gps.object_name !== "TPV") {
    return;
  }

  // read fields
  const gps_tpv_datum = gps_datum.gps.object.tpv_record;
  const genTime = gps_tpv_datum["time"];
  const lat = gps_tpv_datum["lat"];
  const lng = gps_tpv_datum["lon"];

  // log
  logInfo(`Rcvd GPS: t=${genTime} lat=${lat} lon=${lng}`);

  /* get the day bucket from generation timestamp */
  const ts = new Date(genTime * 1000);
  const date = ts.toISOString().split("T")[0];
  const hour =
    ts
      .toISOString()
      .split("T")[1]
      .split(":")[0] + ":00";

  /* construct the JSON object */
  const data = {
    "sec-index": {
      [genTime]: {
        lat: lat,
        lng: lng,
      },
    },
  };

  // path
  const path =
    `/bookmarks/isoblue/device-index/${isoblueId}/location/day-index/${date}/` +
    `hour-index/${hour}`;

  // put
  conn
    .put({
      tree,
      path,
      data,
    })
    .catch(err => {
      logError(err);
    });
}

// Process heartbeat message
function handleHBmsg(conn, m) {
  const key_split = m.key.toString().split(":");
  const isoblueId = key_split[1]; // ISOBlue ID
  var hb_datum;
  try {
    hb_datum = type_hb.fromBuffer(m.value); // deserialize
  } catch (err) {
    logError("Failed to deserialize a heartbeat message.");
    return;
  }

  /* read each field */
  const genTime = hb_datum["timestamp"];
  const cellrssi = hb_datum["cellns"];
  const wifirssi = hb_datum["wifins"];
  const statled = hb_datum["statled"];
  const netled = hb_datum["netled"];

  // log
  logInfo(`Rcvd HB: t=${genTime}`);

  /* get the day bucket from generation timestamp */
  const ts = new Date(genTime * 1000);
  const date = ts.toISOString().split("T")[0];
  const hour =
    ts
      .toISOString()
      .split("T")[1]
      .split(":")[0] + ":00";

  /* we have a heartbeat message, get the current (recv) timestamp */
  const recTime = Date.now() / 1000;

  /* construct the JSON object */
  const data = {
    "sec-index": {
      [genTime]: {
        genTime: genTime,
        recTime: recTime,
        interfaces: [
          { type: "cellular", rssi: cellrssi },
          { type: "wifi", rssi: wifirssi },
        ],
        ledStatuses: [
          { name: "net", status: netled },
          { name: "stat", status: statled },
        ],
      },
    },
  };

  // path
  var path =
    `/bookmarks/isoblue/device-index/${isoblueId}/heartbeat/day-index/${date}/` +
    `hour-index/${hour}`;

  // put
  conn
    .put({
      tree,
      path,
      data,
    })
    .catch(err => {
      logError(err);
    });
}

oada.connect(connectionArgs).then(conn => {
  logInfo("Connected to OADA server.");

  var consumer = new kafka.KafkaConsumer(
    {
      //debug: "all",
      "group.id": kafka_group_id,
      "metadata.broker.list": kafka_broker,
      "enable.auto.commit": false,
    },
    {
      "auto.offset.reset": "latest",
    },
    { encoding: "buffer" },
  );

  //log debug messages
  consumer.on("event.log", function(log) {
    logInfo(log);
  });

  // log error messages
  consumer.on("event.error", function(err) {
    logError("Error from consumer");
    logError(err);
  });

  // ready
  consumer.on("ready", function(args) {
    logInfo("Kafka ready.");
    logInfo(args);
    consumer.subscribe(topics);
    consumer.consume();
  });

  // handle data
  consumer.on("data", function(m) {
    var key_split = m.key.toString().split(":");
    if (key_split[0] == "gps") {
      handleGPSmsg(conn, m);
    } else if (key_split[0] == "hb") {
      handleHBmsg(conn, m);
    }
    return;
  });

  // connect
  consumer.connect();
});
