// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var test = require('tap').test,
    uuid = require('node-uuid'),
    SOCKET = '/tmp/.' + uuid(),
    util = require('util'),
    async = require('async'),
    Factory = require('wf').Factory,
    WorkflowRedisBackend = require('../lib/workflow-moray-backend');

var test = require('tap').test,
    uuid = require('node-uuid'),
    SOCKET = '/tmp/.' + uuid(),
    util = require('util'),
    async = require('async'),
    Factory = require('wf').Factory,
    WorkflowMorayBackend = require('../lib/workflow-moray-backend');

var backend, factory;

var aWorkflow, aJob, anotherJob;

var helper = require('./helper'),
    config = helper.config(),
    runnerId = config.runner.identifier;

test('setup', function (t) {
  console.time('Moray Backend');
  backend = new WorkflowMorayBackend(config.backend.opts);
  t.ok(backend, 'backend ok'); 
  backend.init(function () {
    t.ok(backend.client, 'backend client ok');
    factory = Factory(backend);
    t.ok(factory, 'factory ok');
    t.end();
  });
});


test('teardown', function (t) {
  async.forEach(['wf_workflows', 'wf_jobs', 'wf_runners'],
    function (bucket, cb) {
      backend._bucketExists(bucket, function (exists) {
        if (exists) {
          backend.client.delBucket(bucket, function (err) {
            t.ifError(err, 'Delete ' + bucket + ' bucket error');
            return cb(err);
          });
        } else {
          return cb(null);
        }
      });
    }, function (err) {
      t.ifError(err, 'Delete buckets error');
      backend.quit(function () {
        console.timeEnd('Moray Backend');
        t.end();
      });

    });
});
