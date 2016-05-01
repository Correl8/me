
var request = require('supertest-as-promised'),
  should = require('chai').should(),
  api = require('../server.js');

describe('API ', function () {
  var url = 'http://localhost:3000' // move this to config
  var sensor = 'testSensor';
  var createdId;
  var stepDelay = 2000; // ms
  this.timeout(7 * stepDelay);
  before(function() {
    request(url).get('/' + sensor + '/init').then(function(res) {
      // console.log(res.body);
      res.should.have.property('body');
      res.body.should.have.property('this', 'succeeded');
    });
  });
  context('GET ', function() {
    describe('/clear ', function() {
      this.slow(stepDelay + 500);
      it('should clear index', function(done) {
        setTimeout(function() {
          return request(url).get('/' + sensor + '/clear').then(function(res) {
            console.log(res.body);
            res.should.have.property('body');
            res.body.should.have.property('this', 'succeeded');
            done();
          }).catch(done);
        }, stepDelay);
      });
    });
    describe('/insert ', function() {
      it('should index new document', function(done) {
        this.slow(2 * stepDelay + 500);
        setTimeout(function() {
          return request(url).get('/' + sensor + '/insert/?foo=bar')
          .expect(200).then(function(res) {
            // console.error(res.body);
            res.should.have.property('body');
            res.body.should.have.property('this', 'succeeded');
            res.body.should.have.property('the', sensor);
            res.body.should.have.property('with');
            res.body.with.should.have.property('foo', 'bar');
            done();
          }).catch(done);
        }, 2 * stepDelay);
      });
    });
    describe('/ ', function() {
      it('should find indexed document', function(done) {
        // try to make sure insert is finished
        this.slow(3 * stepDelay + 500);
        setTimeout(function() {
          request(url).get('/' + sensor + '/')
          .expect(200).then(function(res) {
            // console.log(res.body.with.hits.hits);
            res.should.have.property('body');
            res.body.should.have.property('this', 'succeeded');
            res.body.should.have.property('with');
            res.body.with.should.have.property('hits');
            res.body.with.hits.should.have.property('hits').with.lengthOf(1);
            createdId = res.body.with.hits.hits[0]._id;
            done();
          }).catch(done);
        }, 3 * stepDelay);
      });
    });
    describe('/delete ', function() {
      it('should delete created document', function(done) {
        this.slow(4 * stepDelay + 500);
        setTimeout(function() {
          return request(url).get('/' + sensor + '/delete/' + createdId)
          .expect(200).then(function(res) {
            // console.log(res.body);
            res.should.have.property('body');
            res.body.should.have.property('this', 'succeeded');
            res.body.should.have.property('by', 'deleting');
            res.body.should.have.property('the', createdId);
            done();
          }).catch(done);
        }, 4 * stepDelay);
      });
    });
    describe('/remove ', function() {
      it('should remove index', function(done) {
        this.slow(5 * stepDelay + 500);
        setTimeout(function() {
          return request(url).get('/' + sensor + '/remove')
          .expect(200).then(function(res) {
            // console.log(res.body);
            res.should.have.property('body');
            res.body.should.have.property('this', 'succeeded');
            res.body.should.have.property('by', 'deleting');
            res.body.should.have.property('the', sensor);
            done();
          }).catch(done);
        }, 5 * stepDelay);
      });
    });
  });
});
