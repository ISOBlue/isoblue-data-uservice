process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
var fs = require('fs');
var avro = require('avsc');
var kafka = require('node-rdkafka');
var oada = require('@oada/oada-cache').default;

var schema = fs.readFileSync('./d_hb.avsc')
var topics = ['debug'];
var domain = 'https://128.46.71.204';
var token = 'abc';
const type = avro.Type.forSchema(JSON.parse(schema));

var tree = {
  bookmarks: {
    _type: 'application/vnd.oada.bookmarks.1+json',
    _rev: "0-0",
    isoblue: {
      _type: 'application/vnd.oada.isoblue.1+json',
      _rev: "0-0",
      "device-index": {
        "*": {
          _type: 'application/vnd.oada.isoblue.device.1+json',
          _rev: "0-0",
          "day-index": {
            "*": {
              _type: 'application/vnd.oada.isoblue.day.1+json',
              _rev: "0-0",
              "hour-index":{
                "*": {
                  _type: 'application/vnd.oada.isoblue.hour.1+json',
                  _rev: "0-0",
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
  domain,
  token,
  cache: false,
}


return oada.connect(connectionArgs).then((conn) => {
  console.log('connected');
   
  var consumer = new kafka.KafkaConsumer({
   'group.id': 'kafka', 
   'metadata.broker.list': 'localhost:9092',
  });

  consumer.connect()
  consumer
    .on('ready', function(message) {
      consumer.subscribe(topics);
      consumer.consume();
    })
    .on('data', function() {
 
      // disregard any message that does not have heartbeat key
      var key_split = message.key.split(':')
      if (key_split[0] != 'hb') {
        return;
      }

      // we have a heartbeat message, get the current (recv) timestamp
      var recTime = new Date.now() / 1000;

      // get the isoblue id
      var isoblueId = key_split[1];

      // setup avro decoder
      var hb_datum = type.fromBuffer(message.value);

      // read each field
      var genTime = hb_datum['timestamp'];
      var cellrssi = hb_datum['cellns'];
      var wifirssi = hb_datum['wifins'];
      var statled = hb_datum['statled'];
      var netled = hb_datum['netled'];

      console.log(genTime, isoblueId, cellrssi, wifirssi, statled, netled)

      // get the day bucket from generation timestamp
      var day = String(new Date(genTime).toISOString().slice(0, 10));
      var hour = String(new Date(genTime).getHours());
      
      if (hour.length == 1) {
        hour = '0' + hour
      }
      
      console.log('day_bucket is:', day, 'hr_bucket is:', hour);

      // construct the JSON object
      var data = {
        ts: {
          'genTime': genTime,
          'recTime': recTime,
          'interfaces': [
            {'type': 'cellular', 'rssi': cellrssi},
            {'type': 'wifi', 'rssi': wifirssi}
          ],
          'ledStatuses': [
            {'name': 'net', 'status': netled},
            {'name': 'stat', 'status': statled}
          ]
        }
      };

      console.log(data);

      /*
      conn.put({
        tree,
        path:`/bookmarks/isoblue/device-index/${isoblueId}/day-index/${date}/hour-index/${hour}/${bucket}/`,
        data,
      });
      */

    });
});



