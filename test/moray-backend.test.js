// Copyright (c) 2012, Joyent, Inc. All rights reserved.
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
    async.forEach(['wf_workflows', 'wf_jobs', 'wf_runners'],
      function (bucket, cb) {
        backend._bucketExists(bucket, function (exists) {
          if (exists) {
            return backend.client.delBucket(bucket, function (err) {
              t.ifError(err, 'Delete ' + bucket + ' bucket error');
              return cb(err);
            });
          } else {
            return cb(null);
          }
        });
      }, function (err) {
        t.ifError(err, 'Delete buckets error');
        backend._createBuckets(function (err) {
          t.ifError(err, 'Create buckets error');
          t.end();
        });
      });
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


test('create job', function (t) {
  factory.job({
    workflow: aWorkflow.uuid,
    target: '/foo/bar',
    params: {
      a: '1',
      b: '2'
    }
  }, function (err, job) {
    console.dir(job);
    console.dir(err);
    t.ifError(err, 'create job error');
    t.ok(job, 'create job ok');
    t.ok(job.exec_after, 'job exec_after');
    t.equal(job.execution, 'queued', 'job queued');
    t.ok(job.uuid, 'job uuid');
    t.ok(util.isArray(job.chain), 'job chain is array');
    t.ok(util.isArray(job.onerror), 'job onerror is array');
    t.ok(
      (typeof (job.params) === 'object' && !util.isArray(job.params)),
      'params ok');
    aJob = job;
    backend.getJobProperty(aJob.uuid, 'target', function (err, val) {
      t.ifError(err, 'get job property error');
      t.equal(val, '/foo/bar', 'property value ok');
      t.end();
    });
  });
});


test('duplicated job target', function (t) {
  factory.job({
    workflow: aWorkflow.uuid,
    target: '/foo/bar',
    params: {
      a: '1',
      b: '2'
    }
  }, function (err, job) {
    t.ok(err, 'duplicated job error');
    t.end();
  });
});


test('job with different params', function (t) {
  factory.job({
    workflow: aWorkflow.uuid,
    target: '/foo/bar',
    params: {
      a: '2',
      b: '1'
    }
  }, function (err, job) {
    t.ifError(err, 'create job error');
    t.ok(job, 'create job ok');
    t.ok(job.exec_after);
    t.equal(job.execution, 'queued');
    t.ok(job.uuid);
    t.ok(util.isArray(job.chain), 'job chain is array');
    t.ok(util.isArray(job.onerror), 'job onerror is array');
    t.ok(
      (typeof (job.params) === 'object' && !util.isArray(job.params)),
      'params ok');
    anotherJob = job;
    t.end();
  });
});


test('next jobs', function (t) {
  backend.nextJobs(0, 1, function (err, jobs) {
    t.ifError(err, 'next jobs error');
    t.equal(jobs.length, 2);
    t.equal(jobs[0], aJob.uuid);
    t.equal(jobs[1], anotherJob.uuid);
    t.end();
  });
});


test('next queued job', function (t) {
  var idx = 0;
  backend.nextJob(function (err, job) {
    t.ifError(err, 'next job error' + idx);
    idx += 1;
    t.ok(job, 'first queued job OK');
    t.equal(aJob.uuid, job.uuid);
    backend.nextJob(idx, function (err, job) {
      t.ifError(err, 'next job error: ' + idx);
      idx += 1;
      t.ok(job, '2nd queued job OK');
      t.notEqual(aJob.uuid, job.uuid);
      backend.nextJob(idx, function (err, job) {
        t.ifError(err, 'next job error: ' + idx);
        t.equal(job, null, 'no more queued jobs');
        t.end();
      });
    });
  });
});


// This is the procedure the backend is using for locking a job.
// The test is just here to illustrate what will happen there.
test('lock job', function (t) {
  var theJob, jobETag;
  backend.client.get('wf_jobs', aJob.uuid, function (err, meta, job) {
    delete job.uuid;
    theJob = job;
    jobETag = meta.etag;
    theJob.runner_id = runnerId;
    theJob.execution = 'running';
    backend.client.put('wf_jobs', aJob.uuid, theJob, {
      etag: jobETag
    }, function (err, meta) {
      t.ifError(err, 'lock job error');
      backend.client.put('wf_jobs', aJob.uuid, theJob, {
        etag: jobETag
      }, function (err, meta) {
        t.ok(err, 'job not locked error');
        // Undo for the next test
        theJob.runner_id = null;
        theJob.execution = 'queued';
        backend.client.put('wf_jobs', aJob.uuid, theJob, function (err, meta) {
          t.ifError(err, 'unlock job error');
          t.end();
        });
      });
    });
  });
});


test('run job', function (t) {
  backend.runJob(aJob.uuid, runnerId, function (err, job) {
    t.ifError(err, 'run job error');
    t.equal(job.runner_id, runnerId, 'run job runner');
    t.equal(job.execution, 'running', 'run job status');
    aJob = job;
    backend.getRunnerJobs(runnerId, function (err, jobs) {
      t.ifError(err, 'get runner jobs err');
      t.equal(jobs.length, 1);
      t.equal(jobs[0], aJob.uuid);
      // If the job is running, it shouldn't be available for nextJob:
      backend.nextJob(function (err, job) {
        t.ifError(err, 'run job next error');
        t.notEqual(aJob.uuid, job.uuid, 'run job next job');
        t.end();
      });
    });
  });
});


test('update job', function (t) {
  aJob.chain_results = [
    {result: 'OK', error: ''},
    {result: 'OK', error: ''}
  ];

  backend.updateJob(aJob, function (err, job) {
    t.ifError(err, 'update job error');
    t.equal(job.runner_id, runnerId, 'update job runner');
    t.equal(job.execution, 'running', 'update job status');
    t.ok(util.isArray(job.chain_results), 'chain_results is array');
    t.equal(2, job.chain_results.length);
    aJob = job;
    t.end();
  });
});


test('update job property', function (t) {
  backend.updateJobProperty(aJob.uuid, 'target', '/foo/baz', function (err) {
    t.ifError(err, 'update job property error');
    backend.getJob(aJob.uuid, function (err, job) {
      t.ifError(err, 'update property get job error');
      t.equal(job.target, '/foo/baz');
      t.end();
    });
  });
});


test('finish job', function (t) {
  aJob.chain_results = [
    {result: 'OK', error: ''},
    {result: 'OK', error: ''},
    {result: 'OK', error: ''},
    {result: 'OK', error: ''}
  ];

  backend.finishJob(aJob, function (err, job) {
    t.ifError(err, 'finish job error');
    t.equivalent(job.chain_results, [
      {result: 'OK', error: ''},
      {result: 'OK', error: ''},
      {result: 'OK', error: ''},
      {result: 'OK', error: ''}
    ], 'finish job results');
    t.ok(!job.runner_id);
    t.equal(job.execution, 'succeeded', 'finished job status');
    aJob = job;
    t.end();
  });
});


test('re queue job', function (t) {
  backend.runJob(anotherJob.uuid, runnerId, function (err, job) {
    t.ifError(err, 're queue job run job error');
    anotherJob.chain_results = JSON.stringify([
      {success: true, error: ''}
    ]);
    backend.queueJob(anotherJob, function (err, job) {
      t.ifError(err, 're queue job error');
      t.ok(!job.runner_id, 're queue job runner');
      t.equal(job.execution, 'queued', 're queue job status');
      anotherJob = job;
      t.end();
    });
  });
});


test('register runner', function (t) {
  var d = new Date();
  t.test('without specific time', function (t) {
    backend.registerRunner(runnerId, function (err) {
      t.ifError(err, 'register runner error');
      backend.getRunner(runnerId, function (err, res) {
        t.ifError(err, 'get runner error');
        t.ok(util.isDate(res), 'runner active at');
        t.ok((res.getTime() >= d.getTime()), 'runner timestamp');
        t.end();
      });
    });
  });
  t.test('with specific time', function (t) {
    backend.registerRunner(runnerId, d.toISOString(), function (err) {
      t.ifError(err, 'register runner error');
      backend.getRunner(runnerId, function (err, timestamp) {
        t.ifError(err, 'backend get runner error');
        t.equivalent(d, timestamp);
        t.end();
      });
    });
  });
});


test('runner active', function (t) {
  var d = new Date();
  backend.runnerActive(runnerId, function (err) {
    t.ifError(err, 'runner active error');
    backend.getRunner(runnerId, function (err, res) {
      t.ifError(err, 'get runner error');
      t.ok((res.getTime() >= d.getTime()), 'runner timestamp');
      t.end();
    });
  });
});


test('get all runners', function (t) {
  backend.getRunners(function (err, runners) {
    t.ifError(err, 'get runners error');
    t.ok(runners, 'runners ok');
    t.ok(runners[runnerId], 'runner id ok');
    t.ok(util.isDate(runners[runnerId]), 'runner timestamp ok');
    t.end();
  });
});


test('idle runner', function (t) {
  t.test('check runner is not idle', function (t) {
    backend.isRunnerIdle(runnerId, function (idle) {
      t.equal(idle, false);
      t.end();
    });
  });
  t.test('set runner as idle', function (t) {
    backend.idleRunner(runnerId, function (err) {
      t.ifError(err);
      t.end();
    });
  });
  t.test('check runner is idle', function (t) {
    backend.isRunnerIdle(runnerId, function (idle) {
      t.equal(idle, true);
      t.end();
    });
  });
  t.test('set runner as not idle', function (t) {
    backend.wakeUpRunner(runnerId, function (err) {
      t.ifError(err);
      t.end();
    });
  });
  t.test('check runner is not idle', function (t) {
    backend.isRunnerIdle(runnerId, function (idle) {
      t.equal(idle, false);
      t.end();
    });
  });
  t.end();
});


test('get workflows', function (t) {
  backend.getWorkflows(function (err, workflows) {
    t.ifError(err, 'get workflows error');
    t.ok(workflows, 'workflows ok');
    t.equal(workflows[0].uuid, aWorkflow.uuid, 'workflow uuid ok');
    t.ok(util.isArray(workflows[0].chain), 'workflow chain ok');
    t.ok(util.isArray(workflows[0].onerror), 'workflow onerror ok');
    t.end();
  });
});


test('get all jobs', function (t) {
  backend.getJobs(function (err, jobs) {
    t.ifError(err, 'get all jobs error');
    t.ok(jobs, 'jobs ok');
    t.ok(util.isArray(jobs[0].chain), 'jobs chain ok');
    t.ok(util.isArray(jobs[0].onerror), 'jobs onerror ok');
    t.ok(util.isArray(jobs[0].chain_results), 'jobs chain_results ok');
    t.ok(
      (typeof (jobs[0].params) === 'object' && !util.isArray(jobs[0].params)),
      'job params ok');
    t.equal(jobs.length, 2);
    t.end();
  });
});


test('get succeeded jobs', function (t) {
  backend.getJobs('succeeded', function (err, jobs) {
    t.ifError(err, 'get succeeded jobs error');
    t.ok(jobs, 'jobs ok');
    t.equal(jobs.length, 1);
    t.equal(jobs[0].execution, 'succeeded');
    t.ok(util.isArray(jobs[0].chain), 'jobs chain ok');
    t.ok(util.isArray(jobs[0].onerror), 'jobs onerror ok');
    t.ok(util.isArray(jobs[0].chain_results), 'jobs chain_results ok');
    t.ok(
      (typeof (jobs[0].params) === 'object' && !util.isArray(jobs[0].params)),
      'job params ok');
    t.end();
  });
});


test('get queued jobs', function (t) {
  backend.getJobs('queued', function (err, jobs) {
    t.ifError(err, 'get queued jobs error');
    t.ok(jobs, 'jobs ok');
    t.equal(jobs.length, 1);
    t.equal(jobs[0].execution, 'queued');
    t.ok(util.isArray(jobs[0].chain), 'jobs chain ok');
    t.ok(util.isArray(jobs[0].onerror), 'jobs onerror ok');
    t.ok(util.isArray(jobs[0].chain_results), 'jobs chain_results ok');
    t.ok(
      (typeof (jobs[0].params) === 'object' && !util.isArray(jobs[0].params)),
      'job params ok');
    t.end();
  });
});


test('add job info', function (t) {
  t.test('to unexisting job', function (t) {
    backend.addInfo(
      uuid(),
      {'10%': 'Task completed step one'},
      function (err) {
        t.ok(err);
        t.equal(typeof (err), 'object');
        t.equal(err.name, 'BackendResourceNotFoundError');
        t.end();
      });
  });
  t.test('to existing job without previous info', function (t) {
    backend.addInfo(
      aJob.uuid,
      {'10%': 'Task completed step one'},
      function (err) {
        t.ifError(err);
        t.end();
      });
  });
  t.test('to existing job with previous info', function (t) {
    backend.addInfo(
      aJob.uuid,
      {'20%': 'Task completed step two'},
      function (err) {
        t.ifError(err);
        t.end();
      });
  });
  t.end();
});


test('get job info', function (t) {
  t.test('from unexisting job', function (t) {
    backend.getInfo(
      uuid(),
      function (err, info) {
        t.ok(err);
        t.equal(typeof (err), 'object');
        t.equal(err.name, 'BackendResourceNotFoundError');
        t.end();
      });
  });
  t.test('from existing job', function (t) {
    backend.getInfo(
      aJob.uuid,
      function (err, info) {
        t.ifError(err);
        t.ok(info);
        t.ok(util.isArray(info));
        t.equal(info.length, 2);
        t.equivalent({'10%': 'Task completed step one'}, info[0]);
        t.equivalent({'20%': 'Task completed step two'}, info[1]);
        t.end();
      });
  });
  t.end();
});


test('teardown', function (t) {
  async.forEach(['wf_workflows', 'wf_jobs', 'wf_runners'],
    function (bucket, cb) {
      backend._bucketExists(bucket, function (exists) {
        if (exists) {
          return backend.client.delBucket(bucket, function (err) {
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
