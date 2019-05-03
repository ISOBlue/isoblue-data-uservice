process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

var fs = require("fs");
var avro = require("avsc");
var kafka = require("node-rdkafka");
var oada = require("@oada/oada-cache").default;

var topics = ["remote", "debug"];
const domain = process.env.OADA_URL;
const kafka_group_id = process.env.KAFKA_GROUP_ID || "isoblue-gps";
var token = "abc";
const type_gps = avro.Type.forSchema(
  JSON.parse(fs.readFileSync("./schema/gps.avsc")),
);
const type_hb = avro.Type.forSchema(
  JSON.parse(fs.readFileSync("./schema/d_hb.avsc")),
);
const kafka_broker = process.env.KAFKA_BROKER;

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

oada.connect(connectionArgs).then(conn => {
  console.log("OADA connected!");

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
    console.log(log);
  });

  // log error messages
  consumer.on("event.error", function(err) {
    console.error("Error from consumer");
    console.error(err);
  });

  // ready
  consumer.on("ready", function(args) {
    console.log("kafka ready.");
    console.log(args);
    consumer.subscribe(topics);
    consumer.consume();
  });

  // Process GPS message
  function handleGPSmsg(m) {
    const key_split = m.key.toString().split(":");
    const isoblueId = key_split[1]; // ISOBlue ID
    var gps_datum;
    try {
      gps_datum = type_gps.fromBuffer(m.value); // deserialize
    } catch (err) {
      console.error("Invalid GPS message.");
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
    console.log(`Rcvd GPS: t=${genTime} lat=${lat} lon=${lng}`);

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
        console.log(err);
      });
  }

  // Process heartbeat message
  function handleHBmsg(m) {
    const key_split = m.key.toString().split(":");
    const isoblueId = key_split[1]; // ISOBlue ID
    var hb_datum;
    try {
      hb_datum = type_hb.fromBuffer(m.value); // deserialize
    } catch (err) {
      console.error("Invalid heartbeat message.");
      return;
    }

    /* read each field */
    const genTime = hb_datum["timestamp"];
    const cellrssi = hb_datum["cellns"];
    const wifirssi = hb_datum["wifins"];
    const statled = hb_datum["statled"];
    const netled = hb_datum["netled"];

    // log
    console.log(`Rcvd HB: t=${genTime}`);

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
        console.log(err);
      });
  }

  // handle data
  consumer.on("data", function(m) {
    /* disregard any message that does not have heartbeat key */
    var key_split = m.key.toString().split(":");
    if (key_split[0] == "gps") {
      handleGPSmsg(m);
    } else if (key_split[0] == "hb") {
      handleHBmsg(m);
    }
    return;
  });

  // connect
  consumer.connect();
});
