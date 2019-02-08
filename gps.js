process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

var fs = require("fs");
var avro = require("avsc");
var kafka = require("node-rdkafka");
var oada = require("@oada/oada-cache").default;

var schema = fs.readFileSync("./schema/gps.avsc");
var topics = ["remote"];
const domain = process.env.OADA_URL;
const kafka_group_id = process.env.KAFKA_GROUP_ID || "isoblue-gps";
var token = "abc";
const type = avro.Type.forSchema(JSON.parse(schema));
const kafka_broker = process.env.KAFKA_BROKER;

var tree = {
  bookmarks: {
    _type: "application/vnd.oada.bookmarks.1+json",
    _rev: "0-0",
    isoblue: {
      _type: "application/vnd.oada.isoblue.1+json",
      _rev: "0-0",
      "device-index": {
        "*": {
          _type: "application/vnd.oada.isoblue.device.1+json",
          _rev: "0-0",
          "day-index": {
            "*": {
              _type: "application/vnd.oada.isoblue.day.1+json",
              _rev: "0-0",
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
};

var connectionArgs = {
  websocket: false,
  domain,
  token,
  cache: false,
};

oada.connect(connectionArgs).then(conn => {
  console.log("OADA connected!");

  var consumer = new kafka.KafkaConsumer({
    //debug: "all",
    "group.id": kafka_group_id,
    "auto.offset.reset": "latest",
    "metadata.broker.list": kafka_broker,
  });

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

  // handle data
  consumer.on("data", function(m) {
    /* disregard any message that does not have heartbeat key */
    var key_split = m.key.toString().split(":");
    if (key_split[0] != "gps") {
      return;
    }

    /* get the isoblue id */
    var isoblueId = key_split[1];

    /* setup avro decoder */
    var gps_datum = type.fromBuffer(m.value);

    if (gps_datum.gps.object_name !== "TPV") {
      return;
    }
    const gps_tpv_datum = gps_datum.gps.object.tpv_record;

    /* read each field */
    var genTime = gps_tpv_datum["time"];
    var lat = gps_tpv_datum["lat"];
    var lng = gps_tpv_datum["lon"];

    // log
    console.log(`Rcvd: t=${genTime} lat=${lat} lon=${lng}`);

    /* get the day bucket from generation timestamp */
    var UTCTimestamp = new Date(genTime * 1000);
    var date = UTCTimestamp.toLocaleDateString()
      .split("/")
      .reverse();
    var tmp = date[2];
    date[2] = date[1];
    date[1] = tmp;
    date = date.join("-");
    var hour =
      UTCTimestamp.toLocaleTimeString("en-US", { hour12: false }).split(
        ":",
      )[0] + ":00";

    /* construct the JSON object */
    var data = {
      gps: {
        [genTime]: {
          lat: lat,
          lng: lng,
        },
      },
    };

    var path =
      `/bookmarks/isoblue/device-index/${isoblueId}/day-index/${date}/` +
      `hour-index/${hour}`;

    console.log(path);

    /* do the PUT */
    conn
      .put({
        tree,
        path,
        data,
      })
      .catch(err => {
        console.log(err);
        throw err;
      });
  });

  // connect
  consumer.connect();
});
