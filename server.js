var express = require('express');
var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace'
});
var INDEX_NAME = 'correl8';

var app = express();

app.get('/:sensor/', function (req, res) {
  console.log('in query...');
  var sensor = req.param('sensor');
  var now = new Date().toISOString();
  query(sensor, req.query, res);
});

app.get('/:sensor/insert', function (req, res) {
  var sensor = req.param('sensor');
  var object = req.query;
  insert(sensor, object, res);
});

app.post('/:sensor/', function (req, res) {
  var sensor = req.param('sensor');
  var object = req.query;
  for (var prop in req.body) {
    object.prop = req.body.prop;
  }
  insert(sensor, object, res);
});

app.put('/:sensor/', function (req, res) {
  var sensor = req.param('sensor');
  var object = req.query;
  for (var prop in req.body) {
    object.prop = req.body.prop;
  }
  insert(sensor, object, res);
});

app.get('/:sensor/init', function (req, res) {
  var sensor = req.param('sensor');
  var object = req.query;
  for (var prop in req.body) {
    object.prop = req.body.prop;
  }
  init(sensor, object, res);
});

app.put('/:sensor/init', function (req, res) {
  var sensor = req.param('sensor');
  var object = req.query;
  for (var prop in req.body) {
    object.prop = req.body.prop;
  }
  init(sensor, object, res);
});

app.post('/:sensor/init', function (req, res) {
  var sensor = req.param('sensor');
  var object = req.query;
  for (var prop in req.body) {
    object.prop = req.body.prop;
  }
  init(sensor, object, res);
});


function init(sensor, object, res) {
  var properties = {timestamp: {type: date, format: 'strict_date_optional_time||epoch_millis'}};
  for (var prop in object) {
    properties[prop] = {type: object[prop]};
  }
  var mappings = {};
  mappings[sensor] = {properties: properties};
  client.index({
    index: INDEX_NAME,
    type: sensor,
    body: {}
  }, function (error, response) {
    if (error) {
      console.warn(error);
      return;
    }
    client.indices.putMapping({
      index: INDEX_NAME,
      type: sensor,
      body: {properties: properties}
    }, function (error, response) {
      if (error) {
        console.warn(error);
        return;
      }
      console.log('Creating mappings for sensor ' + sensor + ': ' + JSON.stringify(mappings) + ', result: ' + JSON.stringify(response));
      res.json(response);
    });
  });
}

function insert(sensor, object, res) {
  var ts = new Date();
  if (object && object.timestamp && Date.parse(object.timestamp)) {
    ts = new Date(object.timestamp);
  }

  client.index(
    {
      index: INDEX_NAME,
      type: sensor,
      timestamp: ts, // .toISOString()
      body: object
    },
    function (error, response) {
      if (error) {
        console.warn(error);
        return;
      }
      console.log(response);
      res.json(response);
    }
  );
}

function query(sensor, params, res) {
  client.index(
    {
      index: INDEX_NAME,
      type: sensor,
      body: params
    },
    function (error, response) {
      if (error) {
        console.warn(error);
        return;
      }
      console.log(response);
      res.json(response);
    }
  );
}

var server = app.listen(3000, function () {

  var host = server.address().address;
  var port = server.address().port;

  console.log('Example app listening at http://%s:%s', host, port);

});
