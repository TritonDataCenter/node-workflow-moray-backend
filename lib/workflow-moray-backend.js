// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var util = require('util'),
    async = require('async'),
    restify = require('restify'),
    Logger = require('bunyan'),
    wf = require('wf'),
    node_uuid = require('node-uuid'),
    WorkflowBackend = wf.WorkflowBackend;

var sprintf = util.format;

var WorkflowMorayBackend = module.exports = function (config) {
    WorkflowBackend.call(this);
    if (config.log) {
        this.log = config.log.child({component: 'wf-moray-backend'});
    } else {
        if (!config.logger) {
            config.logger = {};
        }

        config.logger.name = 'wf-moray-backend';
        config.logger.serializers = {
            err: Logger.stdSerializers.err
        };

        config.logger.streams = config.logger.streams || [ {
            level: 'info',
            stream: process.stdout
        }];

        this.log = new Logger(config.logger);
    }
    this.config = config;
    this.client = null;
};

util.inherits(WorkflowMorayBackend, WorkflowBackend);

WorkflowMorayBackend.prototype.init = function (callback) {
    var self = this,
        url = self.config.url || 'http://127.0.0.1:8080',
        moray = require('moray-client');

    self.client = moray.createClient({
        url: url,
        log: self.log,
        connectTimeout: self.config.connectTimeout
    });

    return self._createBuckets(callback);
};


// workflow - Workflow object
// callback - f(err, workflow)
WorkflowMorayBackend.prototype.createWorkflow = function (workflow, callback) {
    var self = this,
        p,
        uuid = workflow.uuid;

    // Will use it as key, avoid trying to save it twice
    delete workflow.uuid;

    // TODO: A good place to verify that the same tasks are not on the chain
    // and into the onerror callback (GH-1).


    for (p in workflow) {
        if (typeof (workflow[p]) === 'object') {
            workflow[p] = JSON.stringify(workflow[p]);
        }
    }

    self.client.put('wf_workflows', uuid, workflow, function (err, meta) {
        // Whatever the result, undo the delete(workflow.uuid) trick
        workflow.uuid = uuid;
        if (err) {
            self.log.error({error: err});
            if (err.code === 'Invalid Argument') {
                return callback(
                  new wf.BackendInvalidArgumentError(err.message));
            } else {
                return callback(new wf.BackendInternalError(err.message));
            }
        } else {
            if (workflow.chain) {
                workflow.chain = JSON.parse(workflow.chain);
            }
            if (workflow.onerror) {
                workflow.onerror = JSON.parse(workflow.onerror);
            }
            return callback(null, workflow);
        }
    });
};


// uuid - Workflow.uuid
// callback - f(err, workflow)
WorkflowMorayBackend.prototype.getWorkflow = function (uuid, callback) {
    var self = this;

    self.client.get('wf_workflows', uuid, function (err, meta, workflow) {
        if (err) {
            self.log.error({error: err});
            if (err.code === 'ResourceNotFound') {
                return callback(new wf.BackendResourceNotFoundError(sprintf(
                  'Workflow with uuid \'%s\' does not exist', uuid)));
            } else {
                return callback(new wf.BackendInternalError(err.message));
            }
        } else {
            if (workflow.chain) {
                workflow.chain = JSON.parse(workflow.chain);
            }
            if (workflow.onerror) {
                workflow.onerror = JSON.parse(workflow.onerror);
            }
            if (workflow.timeout) {
                workflow.timeout = Number(workflow.timeout);
            }
            // We're saving the uuid as key,
            // need to add it back to the workflow:
            workflow.uuid = uuid;
            return callback(null, workflow);
        }
    });
};


// workflow - the workflow object
// callback - f(err, boolean)
WorkflowMorayBackend.prototype.deleteWorkflow = function (workflow, callback) {
    var self = this;
    self.client.del('wf_workflows', workflow.uuid, function (err) {
        if (err) {
            self.log.error({error: err});
            return callback(new wf.BackendInternalError(err.message));
        } else {
            return callback(null, true);
        }
    });
};


// workflow - update workflow object.
// callback - f(err, workflow)
WorkflowMorayBackend.prototype.updateWorkflow = function (workflow, callback) {
    var self = this,
        p,
        uuid = workflow.uuid;

    // Will use it as key, avoid trying to save it twice
    delete workflow.uuid;

    // TODO: A good place to verify that the same tasks are not on the chain
    // and into the onerror callback (GH-1).


    for (p in workflow) {
        if (typeof (workflow[p]) === 'object') {
            workflow[p] = JSON.stringify(workflow[p]);
        }
    }

    self.client.get('wf_workflows', uuid, function (err, meta, aWorkflow) {
        if (err) {
            self.log.error({error: err});
            // Whatever the result, undo the delete(workflow.uuid) trick
            workflow.uuid = uuid;
            if (err.code === 'ResourceNotFound') {
                return callback(new wf.BackendResourceNotFoundError(sprintf(
                  'Workflow with uuid \'%s\' does not exist', uuid)));
            } else {
                return callback(new wf.BackendInternalError(err.message));
            }
        } else {
            return self.client.put('wf_workflows', uuid, workflow,
                function (err, meta) {
                    // Whatever the result, undo the delete(workflow.uuid) trick
                    workflow.uuid = uuid;
                    if (err) {
                        self.log.error({error: err});
                        if (err.code === 'Invalid Argument') {
                            return callback(
                              new wf.BackendInvalidArgumentError(err.message));
                        } else {
                            return callback(
                              new wf.BackendInternalError(err.message));
                        }
                    } else {
                        if (workflow.chain) {
                            workflow.chain = JSON.parse(workflow.chain);
                        }
                        if (workflow.onerror) {
                            workflow.onerror = JSON.parse(workflow.onerror);
                        }
                        return callback(null, workflow);
                      }
                });
        }
    });
};


// job - Job object
// callback - f(err, job)
WorkflowMorayBackend.prototype.createJob = function (job, callback) {
    if (typeof (job) === 'undefined') {
        return callback(new wf.BackendInternalError(
              'WorkflowMorayBackend.createJob job(Object) required'));
    }
    var self = this,
        uuid = job.uuid,
        p;

    delete job.uuid;

    for (p in job) {
        if (typeof (job[p]) === 'object') {
            job[p] = JSON.stringify(job[p]);
        }
    }
    job.created_at = (job.created_at) ?
          new Date(job.created_at).getTime() : new Date().getTime();
    job.exec_after = (job.exec_after) ?
          new Date(job.exec_after).getTime() : new Date().getTime();
    job.execution = job.execution || 'queued';

    return self._putJob(uuid, job, function (err, meta, job) {
        return callback(err, job);
    });
};


// uuid - Job.uuid
// callback - f(err, job)
WorkflowMorayBackend.prototype.getJob = function (uuid, callback) {
    if (typeof (uuid) === 'undefined') {
        return callback(new wf.BackendInternalError(
              'WorkflowMorayBackend.getJob uuid(String) required'));
    }
    var self = this;
    return self.client.get('wf_jobs', uuid, function (err, meta, job) {
        if (err) {
            self.log.error({error: err});
            if (err.code === 'ResourceNotFound') {
                return callback(new wf.BackendResourceNotFoundError(sprintf(
                  'Job with uuid \'%s\' does not exist', uuid)));
            } else {
                return callback(new wf.BackendInternalError(err.message));
            }
        } else {
            job.uuid = uuid;
            return self._decodeJob(job, function (j) {
                return callback(null, j);
            });
        }
    });
};


// Get a single job property
// uuid - Job uuid.
// prop - (String) property name
// cb - callback f(err, value)
WorkflowMorayBackend.prototype.getJobProperty = function (uuid, prop, cb) {
    if (typeof (uuid) === 'undefined') {
        return cb(new wf.BackendInternalError(
              'WorkflowMorayBackend.getJobProperty uuid(String) required'));
    }
    if (typeof (prop) === 'undefined') {
        return cb(new wf.BackendInternalError(
              'WorkflowMorayBackend.getJobProperty prop(String) required'));
    }
    var self = this;
    return self.getJob(uuid, function (err, job) {
        if (err) {
            self.log.error({error: err});
            return cb(err);
        } else {
            return cb(null, job[prop]);
        }
    });
};


// job - the job object
// callback - f(err) called with error in case there is a duplicated
// job with the same target and same params
WorkflowMorayBackend.prototype.validateJobTarget = function (job, callback) {
    if (typeof (job) === 'undefined') {
        return callback(new wf.BackendInternalError(
              'WorkflowMorayBackend.validateJobTarget job(Object) required'));
    }
    var self = this,
        filter;
    // If no target is given, we don't care:
    if (!job.target) {
        return callback(null);
    }

    filter = '(&(target=' + job.target +
          ')(|(execution=queued)(execution=running)))';
    return self.client.search('wf_jobs', filter, function (err, data) {
        if (err) {
            self.log.error({error: err});
            return callback(new wf.BackendInternalError(err.message));
        }
        var keys = Object.keys(data);
        if (keys.length === 0) {
            return callback(null);
        } else {
            return async.forEach(keys, function (k, cb) {
                if (data[k].params &&
                    self._areParamsEqual(job.params,
                      JSON.parse(data[k].params))) {
                    cb(new wf.BackendInvalidArgumentError(
                          'Another job with the same target' +
                          ' and params is already queued'));
                } else {
                    cb(null);
                }
            }, function (err) {
                if (err) {
                    self.log.error({error: err});
                    return callback(err);
                } else {
                    return callback(null);
                }
            });
        }
    });
};


// Get the next queued job.
// index - Integer, optional. When given, it'll get the job at index position
//         (when not given, it'll return the job at position zero).
// callback - f(err, job)
WorkflowMorayBackend.prototype.nextJob = function (index, callback) {
    var self = this,
        now = new Date().getTime(),
        filter = '(&(exec_after<=' + now + ')(|(execution=queued)))';

    // TODO: offset/limit for searches is pending on moray.
    if (typeof (index) === 'function') {
        callback = index;
        index = 0;
    }

    return self.client.search('wf_jobs', filter, {
        'x-moray-sort': '+created_at'
    }, function (err, data) {
        if (err) {
            self.log.error({err: err});
            return callback(new wf.BackendInternalError(err.message));
        }

        var keys = Object.keys(data);
        if (keys.length === 0 || !data[keys[index]]) {
            return callback(null, null);
        } else {
            return self._decodeJob(data[keys[index]], function (job) {
                job.uuid = keys[index];
                return callback(null, job);
            });
        }
    });
};


// Get the given number of queued jobs uuids.
// - start - Integer - Position of the first job to retrieve
// - stop - Integer - Position of the last job to retrieve, _included_
// - callback - f(err, jobs)
WorkflowMorayBackend.prototype.nextJobs = function (start, stop, callback) {
    var self = this,
        filter = '(execution=queued)';

    // TODO: offset/limit for searches is pending on moray,
    // for now it's pseudo-implemented here.
    return self.client.search('wf_jobs', filter, {
        'x-moray-sort': '+created_at'
    }, function (err, data) {
        if (err) {
            self.log.error({err: err});
            return callback(new wf.BackendInternalError(err.message));
        }

        var keys = Object.keys(data),
            offset,
            limit,
            scrap;

        if (keys.length === 0) {
            return callback(null, null);
        } else {
            if (start < 0) {
                offset = 0;
            } else if (start > keys.length) {
                offset = keys.length;
            }

            if (stop <= 0) {
                return callback(null, null);
            } else {
                limit = stop;
                scrap = keys.splice(offset, limit);
                return callback(null, scrap);
            }
        }
    });
};


// Lock a job, mark it as running by the given runner, update job status.
// uuid - the job uuid (String)
// runner_id - the runner identifier (String)
// callback - f(err, job) callback will be called with error if something
//            fails, otherwise it'll return the updated job using getJob.
WorkflowMorayBackend.prototype.runJob = function (uuid, runner_id, callback) {
    if (typeof (uuid) === 'undefined') {
        return callback(new wf.BackendInternalError(
              'WorkflowMorayBackend.runJob uuid(String) required'));
    }

    var self = this,
        theJob,
        etag;

    return self.client.get('wf_jobs', uuid, function (err, meta, job) {
        if (err) {
            self.log.error({err: err});
            if (err.code === 'ResourceNotFound') {
                return callback(new wf.BackendResourceNotFoundError(sprintf(
                  'Job with uuid \'%s\' does not exist', uuid)));
            } else {
                return callback(new wf.BackendInternalError(err.message));
            }
        } else {
            return self._decodeJob(job, function (j) {
                theJob = j;
                if (theJob.execution !== 'queued') {
                    return callback(new wf.BackendPreconditionFailedError(
                        'Only queued jobs can run'));
                } else if (theJob.runner_id) {

                    return callback(new wf.BackendPreconditionFailedError(
                        'Job is already locked by runner: ' + job.runner_id));
                } else {
                    theJob.execution = 'running';
                    theJob.runner_id = runner_id;
                    return self._putJob(uuid, theJob, {
                        etag: etag
                    }, function (err, job, meta) {
                        return callback(err, theJob);
                    });
                }
            });
        }
    });
};


// Unlock the job, mark it as finished, update the status, add the results
// for every job's task.
// job - the job object. It'll be saved to the backend with the provided
//       properties.
// callback - f(err, job) callback will be called with error if something
//            fails, otherwise it'll return the updated job using getJob.
WorkflowMorayBackend.prototype.finishJob = function (job, callback) {
    if (typeof (job) === 'undefined') {
        return callback(new wf.BackendInternalError(
              'WorkflowMorayBackend.finishJob job(Object) required'));
    }

    var self = this,
        p;

    for (p in job) {
        if (typeof (job[p]) === 'object') {
            job[p] = JSON.stringify(job[p]);
        }
    }

    return self.getJob(job.uuid, function (err, aJob) {
        if (err) {
            self.log.error({error: err});
            return callback(err);
        } else {
            return self._decodeJob(aJob, function (j) {
                if (j.execution !== 'running') {
                    self.log.error({
                        err: { job: j }
                    }, 'Trying to finish a not running job');
                    return callback(new wf.BackendPreconditionFailedError(
                      'Only running jobs can be finished'));
                } else {
                    var uuid = job.uuid;
                    delete job.uuid;
                    if (job.execution === 'running') {
                        job.execution = 'succeeded';
                    }
                    job.runner_id = null;
                    return self._putJob(uuid, job, {},
                      function (err, job, meta) {
                        return callback(err, job);
                    });
                }
            });
        }
    });
};


// Update the job while it is running with information regarding progress
// job - the job object. It'll be saved to the backend with the provided
//       properties.
// callback - f(err, job) callback will be called with error if something
//            fails, otherwise it'll return the updated job using getJob.
WorkflowMorayBackend.prototype.updateJob = function (job, callback) {
    if (typeof (job) === 'undefined') {
        return callback(new wf.BackendInternalError(
              'WorkflowMorayBackend.updateJob job(Object) required'));
    }

    var self = this,
        p,
        uuid = job.uuid;

    delete job.uuid;

    for (p in job) {
        if (typeof (job[p]) === 'object') {
            job[p] = JSON.stringify(job[p]);
        }
    }

    return self._putJob(uuid, job, function (err, job, meta) {
        return callback(err, job);
    });
};

// Update only the given Job property. Intendeed to prevent conflicts with
// two sources updating the same job at the same time, but different properties
// uuid - the job's uuid
// prop - the name of the property to update
// val - value to assign to such property
// callback - f(err) called with error if something fails, otherwise with null.
WorkflowMorayBackend.prototype.updateJobProperty = function (
    uuid,
    prop,
    val,
    callback)
{
    if (typeof (uuid) === 'undefined') {
        return callback(new wf.BackendInternalError(
              'WorkflowMorayBackend.updateJobProperty uuid(String) required'));
    }

    if (typeof (prop) === 'undefined') {
        return callback(new wf.BackendInternalError(
              'WorkflowMorayBackend.updateJobProperty prop(String) required'));
    }

    if (typeof (val) === 'undefined') {
        return callback(new wf.BackendInternalError(
              'WorkflowMorayBackend.updateJobProperty val required'));
    }

    var self = this;

    if (typeof (val) === 'object') {
        val = JSON.stringify(val);
    }

    return self.getJob(uuid, function (err, aJob) {
        if (err) {
            self.log.error({err: err});
            return callback(err);
        } else {
            if (prop === 'created_at' && typeof (val) === 'string') {
                val = new Date(val).getTime();
            }
            if (prop === 'exec_after' && typeof (val) === 'string') {
                val = new Date(val).getTime();
            }
            aJob[prop] = val;
            aJob.uuid = uuid;
            return self.updateJob(aJob, callback);
        }
    });
};


// Queue a job which has been running; i.e, due to whatever the reason,
// re-queue the job. It'll unlock the job, update the status, add the
// results for every finished task so far ...
// job - the job Object. It'll be saved to the backend with the provided
//       properties to ensure job status persistence.
// callback - f(err, job) callback will be called with error if something
//            fails, otherwise it'll return the updated job using getJob.
WorkflowMorayBackend.prototype.queueJob = function (job, callback) {
    if (typeof (job) === 'undefined') {
        return callback(new wf.BackendInternalError(
              'WorkflowMorayBackend.queueJob job(Object) required'));
    }

    var self = this,
        p;

    for (p in job) {
        if (typeof (job[p]) === 'object') {
            job[p] = JSON.stringify(job[p]);
        }
    }

    return self.getJob(job.uuid, function (err, aJob) {
        if (err) {
            self.log.error({error: err});
            return callback(err);
        } else {
            return self._decodeJob(aJob, function (j) {
                if (typeof (j.created_at) === 'string') {
                    j.created_at = new Date(j.created_at).getTime();
                }
                if (typeof (j.exec_after) === 'string') {
                    j.exec_after = new Date(j.exec_after).getTime();
                }
                if (j.execution !== 'running') {
                    return callback(new wf.BackendPreconditionFailedError(
                      'Only running jobs can be queued again'));
                } else {
                    var uuid = job.uuid;
                    delete job.uuid;
                    job.execution = 'queued';
                    job.runner_id = null;
                    return self._putJob(uuid, job, function (err, job, meta) {
                        return callback(err, job);
                    });
                }
            });
        }
    });
};


// Register a runner on the backend and report it's active:
// - runner_id - String, unique identifier for runner.
// - active_at - ISO String timestamp. Optional. If none is given, current time
// - callback - f(err)
WorkflowMorayBackend.prototype.registerRunner = function (
    runner_id,
    active_at,
    callback
) {
    var self = this;
    if (typeof (active_at) === 'function') {
        callback = active_at;
        active_at = new Date().toISOString();
    }
    self.client.put('wf_runners', runner_id, {
        active_at: active_at,
        idle: false
    }, function (err, meta) {
        if (err) {
            self.log.error({err: err});
            return callback(new wf.BackendInternalError(err));
        } else {
            return callback(null);
        }
    });
};


// Report a runner remains active:
// - runner_id - String, unique identifier for runner. Required.
// - active_at - ISO String timestamp. Optional. If none is given, current time
// - callback - f(err)
WorkflowMorayBackend.prototype.runnerActive = function (
  runner_id,
  active_at,
  callback
) {
    var self = this;
    return self.registerRunner(runner_id, active_at, callback);
};


// Get the given runner id details
// - runner_id - String, unique identifier for runner. Required.
// - callback - f(err, runner)
WorkflowMorayBackend.prototype.getRunner = function (runner_id, callback) {
    var self = this;
    self.client.get('wf_runners', runner_id, function (err, meta, runner) {
        if (err) {
            self.log.error({err: err});
            if (err.code === 'ResourceNotFound') {
                return callback(new wf.BackendResourceNotFoundError(sprintf(
                  'Runner with id \'%s\' does not exist', runner_id)));
            } else {
                return callback(new wf.BackendInternalError(err.message));
            }
        } else {
            return callback(null, new Date(runner.active_at));
        }
    });
};


// Get all the registered runners:
// - callback - f(err, runners)
WorkflowMorayBackend.prototype.getRunners = function (callback) {
    var self = this,
        req = self.client.keys('wf_runners', {limit: 100}),
        _keys = [],
        error = null;

    req.on('keys', function (keys) {
        _keys = _keys.concat(Object.keys(keys));
        if (req.hasMoreKeys()) {
            req.next();
        }
    });

    req.on('error', function (err) {
        self.log.error({error: err});
        error = new wf.BackendInternalError(err.message);
    });

    req.on('end', function () {
        if (error) {
            return callback(error);
        } else {
            var theRunners = {};
            return async.forEachLimit(_keys, 5, function (runner_id, cb) {
                self.client.get('wf_runners', runner_id,
                  function (err, meta, runner) {
                    if (err) {
                        self.log.error({error: err});
                        cb(err);
                    } else {
                        theRunners[runner_id] = new Date(runner.active_at);
                        cb();
                    }
                });
            }, function (err) {
                if (err) {
                    return callback(new wf.BackendInternalError(err.message));
                } else {
                    return callback(null, theRunners);
                }
            });
        }
    });
};


// Set a runner as idle:
// - runner_id - String, unique identifier for runner
// - callback - f(err)
WorkflowMorayBackend.prototype.idleRunner = function (runner_id, callback) {
    var self = this;
    self.client.get('wf_runners', runner_id, function (err, meta, runner) {
        if (err) {
            self.log.error({error: err});
            if (err.code === 'ResourceNotFound') {
                return callback(new wf.BackendResourceNotFoundError(sprintf(
                  'Runner with id \'%s\' does not exist', runner_id)));
            } else {
                return callback(new wf.BackendInternalError(err.message));
            }
        } else {
            return self.client.put('wf_runners', runner_id, {
                active_at: runner.active_at,
                idle: true
            }, function (err, meta) {
                    if (err) {
                        self.log.error({error: err});
                        return callback(new wf.BackendInternalError(err));
                    } else {
                        return callback(null);
                    }
            });
        }
    });
};


// Check if the given runner is idle
// - runner_id - String, unique identifier for runner
// - callback - f(boolean)
WorkflowMorayBackend.prototype.isRunnerIdle = function (runner_id, callback) {
    var self = this;
    self.client.get('wf_runners', runner_id, function (err, meta, runner) {
        return callback((err || runner.idle));
    });
};


// Remove idleness of the given runner
// - runner_id - String, unique identifier for runner
// - callback - f(err)
WorkflowMorayBackend.prototype.wakeUpRunner = function (runner_id, callback) {
    var self = this;
    return self.registerRunner(runner_id, callback);
};


// Get all jobs associated with the given runner_id
// - runner_id - String, unique identifier for runner
// - callback - f(err, jobs). `jobs` is an array of job's UUIDs.
//   Note `jobs` will be an array, even when empty.
WorkflowMorayBackend.prototype.getRunnerJobs = function (runner_id, callback) {
    var self = this,
        filter = '(runner_id=' + runner_id + ')';

    return self.client.search('wf_jobs', filter, function (err, data) {
        if (err) {
            self.log.error({error: err});
            return callback(new wf.BackendInternalError(err.message));
        }
        return callback(null, Object.keys(data));
    });
};


// Get all the workflows:
// - callback - f(err, workflows)
WorkflowMorayBackend.prototype.getWorkflows = function (callback) {
    var self = this,
        req = self.client.keys('wf_workflows', {limit: 100}),
        _keys = [],
        error = null,
        workflows = [];

    req.on('keys', function (keys) {
        _keys = _keys.concat(Object.keys(keys));
        if (req.hasMoreKeys()) {
            req.next();
        }
    });

    req.on('error', function (err) {
        self.log.error({err: err});
        error = new wf.BackendInternalError(err.message);
    });

    req.on('end', function () {
        if (error) {
            return callback(error);
        } else {
            return async.forEachLimit(_keys, 5, function (uuid, cb) {
                self.client.get('wf_workflows', uuid,
                  function (err, meta, workflow) {
                    if (err) {
                        self.log.error({err: err});
                        cb(err);
                    } else {
                        workflow.uuid = uuid;
                        if (workflow.chain) {
                            workflow.chain = JSON.parse(workflow.chain);
                        }
                        if (workflow.onerror) {
                            workflow.onerror = JSON.parse(workflow.onerror);
                        }
                        workflows.push(workflow);
                        cb();
                    }
                });
            }, function (err) {
                if (err) {
                    return callback(new wf.BackendInternalError(err.message));
                } else {
                    return callback(null, workflows);
                }
            });
        }
    });
};


// Get all the jobs:
// - params - JSON Object. Can include the value of the job's "execution"
//   status, and any other key/value pair to search for into job's params.
//   - execution - String, the execution status for the jobs to return.
//                 Return all jobs if no execution status is given.
// - callback - f(err, jobs)
WorkflowMorayBackend.prototype.getJobs = function (params, callback) {
    var self = this,
        executions = ['queued', 'failed', 'succeeded', 'canceled', 'running'],
        error = null,
        jobs = [],
        req,
        _keys = [],
        filter,
        execution;


    if (typeof (params) === 'object') {
        execution = params.execution;
        delete params.execution;
    }

    if (typeof (params) === 'function') {
        callback = params;
    }

    if (typeof (execution) === 'undefined') {
        req = self.client.keys('wf_jobs', {limit: 100});

        req.on('keys', function (keys) {
            _keys = _keys.concat(Object.keys(keys));
            if (req.hasMoreKeys()) {
                req.next();
            }
        });

        req.on('error', function (err) {
            self.log.error({err: err});
            error = new wf.BackendInternalError(err.message);
        });

        return req.on('end', function () {
            if (error) {
                return callback(error);
            } else {
                return async.forEachLimit(_keys, 5, function (uuid, cb) {
                    self.client.get('wf_jobs', uuid, function (err, meta, job) {
                        if (err) {
                            self.log.error({err: err});
                            cb(err);
                        } else {
                            job.uuid = uuid;
                            self._decodeJob(job, function (j) {
                                jobs.push(j);
                                cb();
                            });
                        }
                    });
                }, function (err) {
                    if (err) {
                        return callback(
                          new wf.BackendInternalError(err.message));
                    } else {
                        if (typeof (params) === 'object' &&
                            Object.keys(params).length > 0) {
                            var theJobs = [];
                            jobs.forEach(function (job) {
                                var matches = true;
                                Object.keys(params).forEach(function (k) {
                                    if (!job.params[k] ||
                                      job.params[k] !== params[k]) {
                                        matches = false;
                                    }
                                });
                                if (matches === true) {
                                    theJobs.push(job);
                                }
                            });
                            return callback(null, theJobs);
                        } else {
                            return callback(null, jobs);
                        }
                    }
                });
            }
        });
    } else if (executions.indexOf(execution !== -1)) {
        filter = '(execution=' + execution + ')';
        return self.client.search('wf_jobs', filter, function (err, data) {
            if (err) {
                self.log.error({error: err});
                return callback(new wf.BackendInternalError(err.message));
            }

            var keys = Object.keys(data);
            if (keys.length === 0) {
                return callback(null, []);
            } else {
                keys.forEach(function (uuid) {
                    self._decodeJob(data[uuid], function (job) {
                        job.uuid = uuid;
                        jobs.push(job);
                    });
                });
                if (typeof (params) === 'object' &&
                  Object.keys(params).length > 0) {
                    var theJobs = [];
                    jobs.forEach(function (job) {
                        var matches = true;
                        Object.keys(params).forEach(function (k) {
                            if (job.params[k] || job.params[k] !== params[k]) {
                                matches = false;
                            }
                        });
                        if (matches === true) {
                            theJobs.push(job);
                        }
                    });
                    return callback(null, theJobs);
                } else {
                    return callback(null, jobs);
                }
            }
        });
    } else {
        return callback(new wf.BackendInvalidArgumentError(
          'excution is required and must be one of' +
          '"queued", "failed", "succeeded", "canceled", "running"'));
    }
};


// Add progress information to an existing job:
// - uuid - String, the Job's UUID.
// - info - Object, {'key' => 'Value'}
// - callback - f(err)
WorkflowMorayBackend.prototype.addInfo = function (uuid, info, callback) {
    var self = this;

    self.client.get('wf_jobs', uuid, function (err, meta, job) {
        if (err) {
            self.log.error({error: err});
            if (err.code === 'ResourceNotFound') {
                return callback(new wf.BackendResourceNotFoundError(sprintf(
                  'Job with uuid \'%s\' does not exist', uuid)));
            } else {
                return callback(new wf.BackendInternalError(err.message));
            }
        } else {
            return self.client.put('wf_jobs_info', node_uuid(), {
                job_uuid: uuid,
                created_at: new Date().getTime(),
                info: JSON.stringify(info)
            }, function (err, meta) {
                if (err) {
                    self.log.error({error: err});
                    if (err.code === 'Invalid Argument') {
                        return callback(
                          new wf.BackendInvalidArgumentError(err.message));
                    } else {
                        return callback(
                          new wf.BackendInternalError(err.message));
                    }
                } else {
                    return callback(null);
                }
            });
        }
    });
};


// Get progress information from an existing job:
// - uuid - String, the Job's UUID.
// - callback - f(err, info)
WorkflowMorayBackend.prototype.getInfo = function (uuid, callback) {
    var self = this,
        filter = '(job_uuid=' + uuid + ')',
        info = [];

    return self.getJob(uuid, function (err, job) {
        if (err) {
            return callback(err);
        } else {
            return self.client.search('wf_jobs_info', filter, {
                'x-moray-sort': '+created_at'
            }, function (err, data) {
                if (err) {
                    self.log.error({error: err});
                    return callback(new wf.BackendInternalError(err.message));
                }

                var keys = Object.keys(data);
                if (keys.length === 0) {
                    return callback(null, []);
                } else {
                    keys.forEach(function (uuid) {
                        var i = data[uuid];
                        if (typeof (i.info) === 'string') {
                            i.info = JSON.parse(i.info);
                        }
                        info.push(i.info);
                    });
                    return callback(null, info);
                }

            });
        }
    });
};



// --- Moray specific methods ---


// name - String, bucket name
// callback - f(boolean)
WorkflowMorayBackend.prototype._bucketExists = function (name, callback) {
    var self = this;
    return self.client.getBucket(name, function (err, bucket) {
        return (err) ? callback(false) : callback(true);
    });
};


// Create all the moray buckets required by the module only if they don't exist
// callback - f(err)
WorkflowMorayBackend.prototype._createBuckets = function (callback) {
    var self = this,
        series = {
        wf_workflows: function (next) {
            return self._bucketExists('wf_workflows', function (exists) {
                if (!exists) {
                    return self.client.putBucket('wf_workflows', {
                        schema: {
                            name: {
                                type: 'string',
                                unique: true
                            }
                        }
                    }, function (err) {
                        if (err) {
                            return next(err);
                        } else {
                            return next(null, true);
                        }
                    });
                } else {
                    return next(null, false);
                }
            });
        },
        wf_jobs: function (next) {
            return self._bucketExists('wf_jobs', function (exists) {
                if (!exists) {
                    return self.client.putBucket('wf_jobs', {
                        schema: {
                            execution: {
                                type: 'string'
                            },
                            workflow_uuid: {
                                type: 'string'
                            },
                            created_at: {
                                type: 'number'
                            },
                            exec_after: {
                                type: 'number'
                            },
                            runner_id: {
                                type: 'string'
                            },
                            target: {
                                type: 'string'
                            }
                        },
                        pre: [
                          function requiredFields(req, cb) {
                              var restify = require('restify');
                              ['name', 'chain', 'workflow_uuid'].forEach(
                                  function (field) {
                                      if (!req.value[field]) {
                                          return cb(
                                            new restify.MissingParameterError(
                                              field + ' is required'));
                                      } else {
                                          return null;
                                      }
                                  });

                              if (!req.value.created_at) {
                                  req.value.created_at = new Date().getTime();
                              }
                              if (!req.value.exec_after) {
                                  req.value.exec_after = new Date().getTime();
                              }

                              if (!req.value.execution) {
                                  req.value.execution = 'queued';
                              }
                              return cb();
                          },
                            function validateExecution(req, cb) {
                                var exec = req.value.execution,
                                    statuses = ['queued',
                                                'failed',
                                                'succeeded',
                                                'canceled',
                                                'running'],
                                    restify = require('restify');
                                if (!exec || statuses.indexOf(exec) !== -1) {
                                    return cb();
                                } else {
                                    return cb(
                                      new restify.InvalidArgumentError(
                                        'execution is invalid'));
                                }
                            },
                            function validateUUID(req, cb) {
                                var UUID_FORMAT = new RegExp('^' + [
                                    '[0-9a-f]{8}',
                                    '[0-9a-f]{4}',
                                    '4[0-9a-f]{3}',
                                    '[89ab][0-9a-f]{3}',
                                    '[0-9a-f]{12}'
                                ].join('-') + '$', 'i'),
                                wf_uuid = req.value.workflow_uuid,
                                restify = require('restify');

                                if (!wf_uuid || wf_uuid.match(UUID_FORMAT)) {
                                    return cb(null);
                                } else {
                                    return cb(new restify.InvalidArgumentError(
                                          'workflow_uuid is invalid'));
                                }
                            },
                            function checkValidDates(req, cb) {
                                var dates = ['exec_after', 'created_at'],
                                    restify = require('restify');
                                dates.forEach(function (d) {
                                    var day = new Date(d);
                                    if (!util.isDate(day)) {
                                        return cb(
                                          new restify.InvalidArgumentError(d +
                                            ' is invalid'));
                                    } else {
                                        return null;
                                    }
                                });
                                return cb(null);
                            }
                        ]
                    }, function (err) {
                        if (err) {
                            return next(err);
                        } else {
                            return next(null, true);
                        }
                    });
                } else {
                    return next(null, false);
                }
            });
        },
        wf_runners: function (next) {
            return self._bucketExists('wf_runners', function (exists) {
                if (!exists) {
                    return self.client.putBucket('wf_runners', {
                        schema: {}
                    }, function (err) {
                        if (err) {
                            return next(err);
                        } else {
                            return next(null, true);
                        }
                    });
                } else {
                    return next(null, false);
                }
            });

        },
        wf_jobs_info: function (next) {
            return self._bucketExists('wf_jobs_info', function (exists) {
                if (!exists) {
                    return self.client.putBucket('wf_jobs_info', {
                        schema: {
                            job_uuid: {
                                type: 'string'
                            },
                            created_at: {
                                type: 'number'
                            }
                        },
                        pre: [
                            function requiredFields(req, cb) {
                                var restify = require('restify');
                                ['info', 'job_uuid'].forEach(function (field) {
                                    if (!req.value[field]) {
                                        return cb(
                                          new restify.MissingParameterError(
                                            field +' is required'));
                                    } else {
                                        return null;
                                    }
                                });

                                if (!req.value.created_at) {
                                    req.value.created_at = new Date().getTime();
                                }
                                return cb();
                            },
                            function validateUUID(req, cb) {
                                var UUID_FORMAT = new RegExp('^' + [
                                    '[0-9a-f]{8}',
                                    '[0-9a-f]{4}',
                                    '4[0-9a-f]{3}',
                                    '[89ab][0-9a-f]{3}',
                                    '[0-9a-f]{12}'
                                ].join('-') + '$', 'i'),
                                job_uuid = req.value.job_uuid,
                                restify = require('restify');

                                if (!job_uuid || job_uuid.match(UUID_FORMAT)) {
                                    return cb(null);
                                } else {
                                    return cb(new restify.InvalidArgumentError(
                                          'job_uuid is invalid'));
                                }
                            },
                            function checkValidDates(req, cb) {
                                var dates = ['created_at'],
                                    restify = require('restify');
                                dates.forEach(function (d) {
                                    var day = new Date(d);
                                    if (!util.isDate(day)) {
                                        return cb(
                                          new restify.InvalidArgumentError(d +
                                            ' is invalid'));
                                    } else {
                                        return null;
                                    }
                                });
                                return cb(null);
                            }
                        ]
                    }, function (err) {
                        if (err) {
                            return next(err);
                        } else {
                            return next(null, true);
                        }
                    });
                } else {
                    return next(null, false);
                }
          });
        }
          };

    async.series(series, function (err, results) {
        if (err) {
            self.log.error({error: err});
            return callback(err);
        } else {
            return callback(null);
        }
    });
};


// Callback - f(err, res);
WorkflowMorayBackend.prototype.quit = function (callback) {
    callback();
};

// Return all the JSON.stringified job properties decoded back to objects
// - job - (object) raw job from redis to decode
// - callback - (function) f(job)
WorkflowMorayBackend.prototype._decodeJob = function (job, callback) {
    if (job.chain && typeof (job.chain) === 'string') {
        job.chain = JSON.parse(job.chain);
    }
    if (job.onerror && typeof (job.onerror) === 'string') {
        job.onerror = JSON.parse(job.onerror);
    }
    if (job.chain_results && typeof (job.chain_results) === 'string') {
        job.chain_results = JSON.parse(job.chain_results);
    }
    if (job.onerror_results && typeof (job.onerror_results) === 'string') {
        job.onerror_results = JSON.parse(job.onerror_results);
    }
    if (job.params && typeof (job.params) === 'string') {
        job.params = JSON.parse(job.params);
    }
    if (job.created_at && typeof (job.created_at) === 'number') {
        job.created_at = new Date(job.created_at).toISOString();
    }
    if (job.exec_after && typeof (job.exec_after) === 'number') {
        job.exec_after = new Date(job.exec_after).toISOString();
    }
    return callback(job);
};


WorkflowMorayBackend.prototype._areParamsEqual = function (a, b) {
    var aKeys = Object.keys(a),
        bKeys = Object.keys(b),
        diff = aKeys.filter(function (k) {
          return (bKeys.indexOf(k) === -1);
        }),
        p;

    // Forget if we just don't have the same number of members:
    if (aKeys.length !== bKeys.length) {
        return false;
    }

    // Forget if we don't have the same keys exactly
    if (diff.length > 0) {
        return false;
    }

    for (p in a) {
        if (b[p] !== a[p]) {
            return false;
        }
    }

    return true;
};

// Used internally by all methods updating a job
// uuid - String
// job - Object Job
// opts - Object moray client conditional update options.
// callback - f(err, job, meta)
WorkflowMorayBackend.prototype._putJob = function (uuid, job, opts, callback) {
    var self = this;
    if (typeof (opts) === 'function') {
        callback = opts;
        opts = {};
    }

    if (typeof (job.created_at) === 'string') {
        job.created_at = new Date(job.created_at).getTime();
    }
    if (typeof (job.exec_after) === 'string') {
        job.exec_after = new Date(job.exec_after).getTime();
    }

    return self.client.put('wf_jobs', uuid, job, opts, function (err, meta) {
        job.uuid = uuid;
        if (err) {
            self.log.error({err: err, job: job}, 'PUT job error');
            if (err.code === 'Invalid Argument') {
                return callback(
                  new wf.BackendInvalidArgumentError(err.message));
            } else if (err.code === 'PreconditionFailed') {
                return callback(new wf.BackendPreconditionFailedError(
                  'Job has been locked by another runner'));
            } else {
                return callback(new wf.BackendInternalError(err.message));
            }
        } else {
            return self._decodeJob(job, function (j) {
              callback(null, j, meta);
          });
        }
    });
};
