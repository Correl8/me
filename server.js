var express = require('express');
var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace'
});
var INDEX_NAME = 'correl8';

// set range for numeric timestamp guessing
var SECONDS_PAST = 60 * 60 * 24 * 365 * 10; // about 10 years in the past
var SECONDS_FUTURE = 60 * 60 * 24; // 24 hours in the future

var app = express();

app.get('/favicon.ico/', function (req, res) {
  res.send();
});

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
  var properties = {timestamp: {type: 'date', format: 'strict_date_optional_time||epoch_millis'}};
  for (var prop in object) {
    // doc_values: store values to disk (reduce heap size)
    properties[prop] = {type: object[prop], doc_values: true};
    // use propietary "text" type for analyzed strings
    if (object[prop] === 'text') {
      properties[prop].type = 'string';
      // doc_values do not currently work with analyzed string fields
      properties[prop].doc_values = false;
    }
    // other strings are not analyzed
    else if (object[prop] === 'string') {
      properties[prop].index = 'not_analyzed';
    }
  }
  var mappings = {};
  mappings[sensor] = {properties: properties};
  client.index({
    index: INDEX_NAME + '-' + sensor,
    type: sensor,
    body: {}
  }, function (error, response) {
    if (error) {
      console.warn(error);
      return;
    }
    client.indices.putMapping({
      index: INDEX_NAME + '-' + sensor,
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
  if (object && object.timestamp) {
    // timestamp is a string that can be parsed by Date.parse
    if (!isNaN(Date.parse(object.timestamp))) {
      ts = new Date(object.timestamp);
    }
    // timestamp is milliseconds within valid range
    else if ((object.timestamp >= (ts.getTime() - SECONDS_PAST * 1000)) &&
             (object.timestamp <= (ts.getTime() - SECONDS_FUTURE * 1000))) {
      ts.setTime(object.timestamp);
    }
    // timestamp is seconds within valid range
    else if ((object.timestamp >= (ts.getTime() / 1000 - SECONDS_PAST)) &&
             (object.timestamp <= (ts.getTime() / 1000 - SECONDS_FUTURE))) {
      ts.setTime(object.timestamp * 1000);
    }
  }
  object.timestamp = ts;

  client.index(
    {
      index: INDEX_NAME + '-' + sensor,
      type: sensor,
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
      index: INDEX_NAME + '-' + sensor,
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
