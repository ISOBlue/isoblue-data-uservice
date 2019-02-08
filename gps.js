process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

var fs = require("fs");
var avro = require("avsc");
var kafka = require("node-rdkafka");
var oada = require("@oada/oada-cache").default;

var schema = fs.readFileSync("./schema/gps.avsc");
var topics = ["remote"];
var domain = "https://128.46.71.204";
var token = "abc";
const type = avro.Type.forSchema(JSON.parse(schema));

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

return oada.connect(connectionArgs).then(conn => {
  console.log("OADA connected!");

  var consumer = new kafka.KafkaConsumer({
    "group.id": "isoblue-gps",
    "auto.offset.reset": "latest",
    "metadata.broker.list": "cloudradio39.ecn.purdue.edu:9092",
    //   'metadata.broker.list': 'localhost:9092',
  });

  consumer.connect();
  consumer
    .on("ready", function() {
      console.log("kafka connected!");
      consumer.subscribe(topics);
      consumer.consume();
    })
    .on("data", function(data) {
      /* disregard any message that does not have heartbeat key */
      var key_split = data.key.toString().split(":");
      if (key_split[0] != "gps") {
        return;
      }

      /* get the isoblue id */
      var isoblueId = key_split[1];

      /* setup avro decoder */
      var gps_datum = type.fromBuffer(data.value);

      //      console.log(gps_datum.gps.object_name)

      if (gps_datum.gps.object_name === "TPV") {
        gps_tpv_datum = gps_datum.gps.object.tpv_record;
        //        console.log(gps_tpv_datum);
      } else {
        return;
      }

      /* read each field */
      var genTime = gps_tpv_datum["time"];
      var lat = gps_tpv_datum["lat"];
      var lng = gps_tpv_datum["lon"];

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

      //var date = String(new Date(genTime * 1000).toISOString().slice(0, 10));
      //var hour = String(new Date(genTime * 1000).toTimeString().slice(0, 3)) + '00';

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
      return conn
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
});
