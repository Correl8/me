var express = require('express');
var elasticsearch = require('elasticsearch');
var bodyParser = require('body-parser');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'trace'
});
var INDEX_NAME = 'correl8';

// set range for numeric timestamp guessing
var SECONDS_PAST = 60 * 60 * 24 * 365 * 10; // about 10 years in the past
var SECONDS_FUTURE = 60 * 60 * 24; // 24 hours in the future

var app = express();
app.use(bodyParser.json());
app.get('/favicon.ico/', function (req, res) {
  res.send();
});

app.get('/:sensor/', function (req, res) {
  // console.log('in query...');
  var sensor = req.params.sensor;
  var now = new Date().toISOString();
  query(sensor, req.query, res);
});

app.get('/:sensor/insert', function (req, res) {
  var sensor = req.params.sensor;
  var object = req.query;
  insert(sensor, object, res);
});

app.post('/:sensor/', function (req, res) {
  var sensor = req.params.sensor;
  var object = req.query;
  for (var prop in req.body) {
    object[prop] = req.body[prop];
  }
  insert(sensor, object, res);
});

app.put('/:sensor/', function (req, res) {
  var sensor = req.params.sensor;
  var object = req.query;
  for (var prop in req.body) {
    object[prop] = req.body[prop];
  }
  insert(sensor, object, res);
});

app.get('/:sensor/init', function (req, res) {
  var sensor = req.params.sensor;
  var object = queryToObject(req.query);
  init(sensor, object, res);
});

app.put('/:sensor/init', function (req, res) {
  var sensor = req.params.sensor;
  var object = req.query;
  for (var prop in req.body) {
    object[prop] = req.body[prop];
  }
  init(sensor, object, res);
});

app.post('/:sensor/init', function (req, res) {
  var sensor = req.params.sensor;
  var object = req.query;
  for (var prop in req.body) {
    object[prop] = req.body[prop];
  }
  init(sensor, object, res);
});

app.delete('/:sensor/', function (req, res) {
  var sensor = req.params.sensor;
  deleteIndex(sensor, res);
});

app.all('/:sensor/delete', function (req, res) {
  var sensor = req.params.sensor;
  deleteIndex(sensor, res);
});

function init(sensor, object, res) {
  var mappings = {};
  var properties = createMapping(object)
  properties.timestamp = {type: 'date', format: 'strict_date_optional_time||epoch_millis'};
  mappings[sensor] = {properties: properties};
  client.index({
    index: INDEX_NAME + '-' + sensor.toLowerCase(),
    type: sensor,
    body: {}
  }, function (error, response) {
    if (error) {
      console.warn(error);
      res.json(error);
      return;
    }
    client.indices.putMapping({
      index: INDEX_NAME + '-' + sensor.toLowerCase(),
      type: sensor,
      body: {properties: properties}
    }, function (error, response) {
      if (error) {
        console.warn(error);
        res.json(error);
        return;
      }
      console.log('Creating mappings for sensor ' + sensor + ': ' + JSON.stringify(mappings) + ', result: ' + JSON.stringify(response));
      res.json(response);
    });
  });
}

function deleteIndex(sensor, res) {
  client.indices.delete(
    {
      index: INDEX_NAME + '-' + sensor.toLowerCase() + '*'
    },
    function (error, response) {
      if (error) {
        console.warn(error);
        res.json(error);
        return;
      }
      console.log(response);
      res.json(response);
    }
  );
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

  var indexName = INDEX_NAME + '-' + sensor.toLowerCase();
  var monthIndex = indexName + '-' + ts.getFullYear() + '-' + (ts.getMonth()+1);
  client.indices.exists({index: indexName}).then(function(response) {
    return client.index(
      {
        // optimistic, assumes monthIndex esists...
        index: indexName,
        type: sensor,
        body: object
      }
    );
  }).then(function(response) {
    client.indices.exists({index: monthIndex}).then(function(response) {
      return client.index(
        {
          // optimistic, assumes monthIndex esists...
          index: monthIndex,
          type: sensor,
          body: object
        }
      );
    });
  }).then(function(response) {
    console.log(response);
    res.json(response);
  }).catch(function(error) {
    if (!client.indices.exists({index: monthIndex}) &&
        !client.indices.exists({index: indexName})) {
      res.json({error: true, message: 'Index for sensor ' + sensor + ' not initialized!'})
    }
    else if (!client.indices.exists({index: monthIndex}) &&
             client.indices.exists({index: indexName})) {
      client.indices.getMapping({index: indexName},
      function (error, result) {
        if (error) {
          console.warn(error);
          res.json(error);
          return;
        }
        // override res.json for index init
        var fakeRes = function() {
          var json = function(data) {
            insert(sensor, obj, res); // recursion!!!
          }
        }
        init(sensor, result, fakeRes);
      });
    }
    else {
      console.warn(error);
      res.json(error);
      return;
    }
  });
}

function query(sensor, params, res) {
  client.index(
    {
      index: INDEX_NAME + '-' + sensor.toLowerCase(),
      type: sensor,
      body: params
    },
    function (error, response) {
      if (error) {
        console.warn(error);
        res.json(error);
        return;
      }
      console.log(response);
      res.json(response);
    }
  );
}

function queryToObject(query) {
  // console.log(query);
  var object = {};
  for (var prop in query) {
    // nested object can be specified in GET urls like
    if (prop.indexOf('.') > 0) {
      var parts = prop.split('.');
      // console.log(parts);
      var parent = object;
      for (var i=0; i<parts.length-1; i++) {
        if (!parent[parts[i]]) {
          parent[parts[i]] = {};
          // console.log(parts[i] + ': ' + typeof(parent[parts[i]]) + ' (' + parts[i-1] + ')');
        }
        parent = parent[parts[i]];
        // console.log(object);
      }
      // var parent = (parts.length < 2) ? object : object[parts[parts.length-2]];
      parent[parts[parts.length-1]] = query[prop];
      // console.log(parts[parts.length-1] + ': ' + typeof(parent[parts[parts.length-1]]));
    }
    else {
      object[prop] = query[prop];
    }
  }
  // console.log('Turned ' + JSON.stringify(query) + ' into ' + JSON.stringify(object));
  return(object);
}

function createMapping(object) {
  var properties = {};
  for (var prop in object) {
    if (typeof(object[prop]) === 'object') {
      // recurse into sub objects
      properties[prop] = {type: 'object', properties: createMapping(object[prop])};
    }
    else {
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
  }
  return properties;
}

var server = app.listen(3000, function () {

  var host = server.address().address;
  var port = server.address().port;

  console.log('Correl8 API listening at http://%s:%s', host, port);

});
