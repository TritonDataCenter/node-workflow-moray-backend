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


test('add a workflow', function (t) {
  factory.workflow({
    name: 'A workflow',
    chain: [ {
      name: 'A Task',
      timeout: 30,
      retry: 3,
      body: function (job, cb) {
        return cb(null);
      }
    }],
    timeout: 180,
    onError: [ {
      name: 'Fallback task',
      body: function (job, cb) {
        return cb('Workflow error');
      }
    }]
  }, function (err, workflow) {
    t.ifError(err, 'add workflow error');
    t.ok(workflow, 'add workflow ok');
    aWorkflow = workflow;
    t.ok(workflow.chain[0].uuid, 'add workflow chain task');
    t.ok(workflow.onerror[0].uuid, 'add workflow onerror task');
    t.end();
  });
});


test('workflow name must be unique', function (t) {
  factory.workflow({
    name: 'A workflow',
    chain: [ {
      name: 'A Task',
      timeout: 30,
      retry: 3,
      body: function (job, cb) {
        return cb(null);
      }
    }],
    timeout: 180,
    onError: [ {
      name: 'Fallback task',
      body: function (job, cb) {
        return cb('Workflow error');
      }
    }]
  }, function (err, workflow) {
    t.ok(err, 'duplicated workflow name err');
    t.end();
  });
});


test('get workflow', function (t) {
  backend.getWorkflow(aWorkflow.uuid, function (err, workflow) {
    t.ifError(err, 'get workflow error');
    t.ok(workflow, 'get workflow ok');
    t.equivalent(workflow, aWorkflow);
    backend.getWorkflow(uuid(), function (err, workflow) {
      t.equal(typeof (err), 'object');
      t.equal(err.name, 'BackendResourceNotFoundError');
      t.ok(err.message.match(/uuid/gi), 'unexisting workflow error');
      t.end();
    });
  });
});


test('update workflow', function (t) {
  aWorkflow.chain.push({
    name: 'Another task',
    body: function (job, cb) {
      return cb(null);
    }.toString()
  });
  aWorkflow.name = 'A workflow name';
  backend.updateWorkflow(aWorkflow, function (err, workflow) {
    t.ifError(err, 'update workflow error');
    t.ok(workflow, 'update workflow ok');
    t.ok(workflow.chain[1].name, 'Updated task ok');
    t.ok(workflow.chain[1].body, 'Updated task body ok');
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
