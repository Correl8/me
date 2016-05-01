var express = require('express');
var hapi = require('humanized-api');
var bodyParser = require('body-parser');
var correl8 = require('correl8');

var app = express();
var jsonParser = bodyParser.json()

app.use(hapi.middleware());

app.get('/favicon.ico/', function (req, res) {
  res.send();
});

app.get('/:sensor/', function (req, res) {
  var sensor = req.params.sensor;
  query(sensor, req.query, res);
});

app.get('/:sensor/insert', function (req, res) {
  var sensor = req.params.sensor;
  var object = req.query;
  insert(sensor, object, res);
});

app.post('/:sensor/', jsonParser, function (req, res) {
  var sensor = req.params.sensor;
  var object = req.query;
  for (var prop in req.body) {
    object[prop] = req.body[prop];
  }
  insert(sensor, object, res);
});

app.put('/:sensor/', jsonParser, function (req, res) {
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

app.put('/:sensor/init', jsonParser, function (req, res) {
  var sensor = req.params.sensor;
  var object = req.query;
  for (var prop in req.body) {
    object[prop] = req.body[prop];
  }
  init(sensor, object, res);
});

app.post('/:sensor/init', jsonParser, function (req, res) {
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

app.all('/:sensor/clear', function (req, res) {
  var sensor = req.params.sensor;
  clear(sensor, res);
});

app.all('/:sensor/delete/:id', function (req, res) {
  var sensor = req.params.sensor;
  var id = req.params.id;
  deleteOne(sensor, id, res);
});

app.all('/:sensor/remove', function (req, res) {
  var sensor = req.params.sensor;
  remove(sensor, res);
});

function init(sensor, object, res) {
  var c8 = correl8(sensor);
  c8.init(object).then(function (results) {
    res.sendHAPISuccess(hapi.HAPI_VERB.create, sensor, results);
  }).catch(function(error) {
    res.sendHAPIFailure(error, 500);
  });
}

function remove(sensor, res) {
  var c8 = correl8(sensor);
  // console.log('Removing index ' + sensor);
  c8.remove().then(function (results) {
    console.log(results);
    res.sendHAPISuccess(hapi.HAPI_VERB.delete, sensor, results);
  }).catch(function(error) {
    console.log(error);
    res.sendHAPIFailure(error, 500);
  });
}

function deleteOne(sensor, id, res) {
  var c8 = correl8(sensor);
  // console.log('Deleting document ' + id);
  c8.deleteOne(id).then(function (results) {
    console.log(results);
    res.sendHAPISuccess(hapi.HAPI_VERB.delete, id, results);
  }).catch(function(error) {
    console.log(error);
    res.sendHAPIFailure(error, 500);
  });
}

function clear(sensor, res) {
  var c8 = correl8(sensor);
  c8.clear().then(function (results) {
    res.sendHAPISuccess(hapi.HAPI_VERB.delete, sensor, results);
  }).catch(function(error) {
    res.sendHAPIFailure(error, 500);
  });
}

function insert(sensor, object, res) {
  var c8 = correl8(sensor);
  c8.insert(object).then(function (results) {
    // console.log(JSON.stringify(results, null, 2));
    res.sendHAPISuccess(hapi.HAPI_VERB.create, sensor, object);
  }).catch(function(error) {
    res.sendHAPIFailure(error, 500);
  });
}

function query(sensor, params, res) {
  var c8 = correl8(sensor);
  c8.search(params).then(function (results) {
    res.sendHAPISuccess(hapi.HAPI_VERB.get, sensor, results);
  }).catch(function(error) {
    res.sendHAPIFailure(error, 500);
  });
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
          console.log(parts[i] + ': ' + typeof(parent[parts[i]]) + ' (' + parts[i-1] + ')');
        }
        parent = parent[parts[i]];
        console.log(object);
      }
      // var parent = (parts.length < 2) ? object : object[parts[parts.length-2]];
      parent[parts[parts.length-1]] = query[prop];
      console.log(parts[parts.length-1] + ': ' + typeof(parent[parts[parts.length-1]]));
    }
    else {
      object[prop] = query[prop];
    }
  }
  // console.log('Turned ' + JSON.stringify(query) + ' into ' + JSON.stringify(object));
  return(object);
}

var server = app.listen(3000, function () {
  var host = server.address().address;
  var port = server.address().port;
  if (!host || (host === '::')) {
    host = 'localhost';
  }
  // console.log('Correl8 API listening at http://%s:%s', host, port);
});
