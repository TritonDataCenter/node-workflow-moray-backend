// Copyright (c) 2013, Joyent, Inc. All rights reserved.
var util = require('util');
var vasync = require('vasync');
var Logger = require('bunyan');
var wf = require('wf');
var node_uuid = require('node-uuid');
var makeEmitter = wf.makeEmitter;
var sprintf = util.format;

var WorkflowMorayBackend = module.exports = function (config) {

    if (typeof (config) !== 'object') {
        throw new TypeError('\'config\' (Object) required');
    }

    var log;
    if (config.log) {
        log = config.log.child({component: 'wf-moray-backend'});
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

        log = new Logger(config.logger);
    }
    var client;

    var backend = {
        log: log,
        config: config,
        client: client,
        quit: function quit(callback) {
            client.close();
            return callback();
        },
        ping: function ping(callback) {
            return client.ping({
                log: log
            }, callback);
        },
        connected: false
    };

    makeEmitter(backend);

    // Private
    // name - String, bucket name
    // callback - f(boolean)
    function _bucketExists(name, callback) {
        return client.getBucket(name, function (err, bucket) {
            if (err) {
                log.debug(util.format('Bucket \'%s\' does not exist', bucket));
            }
            return (err) ? callback(false) : callback(true);
        });
    }

    backend._bucketExists = _bucketExists;

    // Private: Create wf_workflows bucket if not exists:
    function _createWfWorkflowsBucket(_, next) {
        return _bucketExists('wf_workflows', function (exists) {
            if (!exists) {
                return client.putBucket('wf_workflows', {
                    index: {
                        name: {
                            type: 'string',
                            unique: true
                        }
                    }
                }, function (err) {
                    if (err) {
                        log.error({err: err},
                            'Error creating bucket wf_workflows');
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


    // Private: create wf_jobs bucket if not exists:
    function _createWfJobsBucket(_, next) {
        return _bucketExists('wf_jobs', function (exists) {
            if (!exists) {
                return client.putBucket('wf_jobs', {
                    index: {
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
                            ['name', 'chain', 'workflow_uuid'].forEach(
                                function (field) {
                                    if (!req.value[field]) {
                                        return cb(
                                            new Error(
                                                field + ' is required'));
                                    } else {
                                        return null;
                                    }
                                });

                            if (!req.value.created_at) {
                                req.value.created_at =
                                    new Date().getTime();
                            }
                            if (!req.value.exec_after) {
                                req.value.exec_after =
                                    new Date().getTime();
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
                                            'running',
                                            'retried'];
                            if (!exec || statuses.indexOf(exec) !== -1) {
                                return cb();
                            } else {
                                return cb(
                                  new Error('execution is invalid'));
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
                            wf_uuid = req.value.workflow_uuid;

                            if (!wf_uuid || wf_uuid.match(UUID_FORMAT)) {
                                return cb(null);
                            } else {
                                return cb(new Error(
                                      'workflow_uuid is invalid'));
                            }
                        },
                        function checkValidDates(req, cb) {
                            var dates = ['exec_after', 'created_at'],
                                util = require('util');
                            dates.forEach(function (d) {
                                var day = new Date(d);
                                if (!util.isDate(day)) {
                                    return cb(
                                      new Error(d + ' is invalid'));
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

    // Private: create wf_runners bucket if not exists:
    function _createWfRunnersBucket(_, next) {
        return _bucketExists('wf_runners', function (exists) {
            if (!exists) {
                return client.putBucket('wf_runners', {
                    index: {
                        idle: {
                            type: 'boolean'
                        },
                        active_at: {
                            type: 'string'
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
    }

    // Private: create wf_jobs_info bucket if not exists:
    function _createWfJobsInfoBucket(_, next) {
        return _bucketExists('wf_jobs_info', function (exists) {
            if (!exists) {
                return client.putBucket('wf_jobs_info', {
                    index: {
                        job_uuid: {
                            type: 'string'
                        },
                        created_at: {
                            type: 'number'
                        }
                    },
                    pre: [
                        function requiredFields(req, cb) {
                            ['info', 'job_uuid'].forEach(function (field) {
                                if (!req.value[field]) {
                                    return cb(
                                      new Error(
                                        field + ' is required'));
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
                            job_uuid = req.value.job_uuid;

                            if (!job_uuid || job_uuid.match(UUID_FORMAT)) {
                                return cb(null);
                            } else {
                                return cb(new Error('job_uuid is invalid'));
                            }
                        },
                        function checkValidDates(req, cb) {
                            var dates = ['created_at'],
                                util = require('util');
                            dates.forEach(function (d) {
                                var day = new Date(d);
                                if (!util.isDate(day)) {
                                    return cb(
                                      new Error(d + ' is invalid'));
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

    // Private
    // Create all the moray buckets required by the module only if they don't
    // exist
    // callback - f(err)
    function _createBuckets(callback) {
        var funcs = [
            _createWfWorkflowsBucket,
            _createWfJobsBucket,
            _createWfRunnersBucket,
            _createWfJobsInfoBucket
        ];

        vasync.pipeline({
            funcs: funcs
        }, function (err, results) {
            if (err) {
                log.error({err: err});
                if (callback) {
                    return callback(err);
                }
            } else {
                // If we hook more listeners to 'connect' event,
                // we need to review this:
                client.removeAllListeners('connect');
                if (callback) {
                    return callback(null);
                }
            }
        });
    }

    backend._createBuckets = _createBuckets;


    // Private.
    // Return all the JSON.stringified job properties decoded back to objects
    // - job - (object) raw job from redis to decode
    // - callback - (function) f(job)
    function _decodeJob(job, callback) {
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
    }


    // Private
    function _areParamsEqual(a, b) {
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
    }


    // Private. Used internally by all methods updating a job
    // uuid - String
    // job - Object Job
    // opts - Object moray client conditional update options.
    // callback - f(err, job, meta)
    function _putJob(uuid, job, opts, callback) {
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

        if (!opts.req_id) {
            opts.req_id = uuid;
        }

        try {
            return client.putObject('wf_jobs', uuid, job, opts,
                    function (err) {
                job.uuid = uuid;
                if (err) {
                    log.error({
                        err: err,
                        job: job,
                        opts: opts
                    }, 'PUT job error');
                    if (err.name === 'UniqueAttributeError') {
                        return callback(
                          new wf.BackendInvalidArgumentError(err.message));
                    } else if (err.name === 'EtagConflictError') {
                        return callback(new wf.BackendPreconditionFailedError(
                          'Job has been locked by another runner'));
                    } else {
                        return callback(
                            new wf.BackendInternalError(err.message));
                    }
                } else {
                    return _decodeJob(job, function (j) {
                        callback(null, j);
                    });
                }
            });

        } catch (e) {
            log.error(e, 'Exception trying to put Job to Moray');
            return callback(new wf.BackendInternalError(e.message));
        }
    }


    // Attempt to connect the client to the backend.
    // On success, create required buckets when necessary.
    function init(callback) {
        var url = config.url || 'http://127.0.0.1:2020';
        var moray = require('moray');

        client = moray.createClient({
            url: url,
            log: log,
            noCache: true,
            connectTimeout: config.connectTimeout,
            reconnect: true,
            retry: { // try to reconnect forever
                maxTimeout: 30000,
                retries: Infinity
            }
        });

        client.on('error', function (err) {
            log.error({err: err}, 'Moray client error');
            backend.connected = false;
            backend.emit('error', err);
        });

        client.on('connectAttempt', function (moray, delay) {
            log.info({
                attempt: moray.toString(),
                delay: delay
            }, 'ring: moray connection attempted: %s', moray.toString());
        });

        backend.client = client;

        return client.on('connect', function () {
            log.info('Successfully connected to moray');
            backend.connected = true;
            backend.emit('connected');
            if (callback) {
                return _createBuckets(callback);
            }
            return _createBuckets();
        });


    }

    backend.init = init;

    // workflow - Workflow object
    // meta - Any additional information to pass to the backend which is not
    //        workflow properties
    // cb - f(err, workflow)
    function createWorkflow(workflow, meta, cb) {
        if (typeof (meta) === 'function') {
            cb = meta;
            meta = {};
        }

        var p;
        var uuid = workflow.uuid;

        if (!meta.req_id) {
            meta.req_id = uuid;
        }

        // Will use it as key, avoid trying to save it twice
        delete workflow.uuid;

        // TODO: A good place to verify that the same tasks are not on the chain
        // and into the onerror callback (GH-1).

        for (p in workflow) {
            if (typeof (workflow[p]) === 'object') {
                workflow[p] = JSON.stringify(workflow[p]);
            }
        }

        client.putObject('wf_workflows', uuid, workflow, meta, function (err) {
            // Whatever the result, undo the delete(workflow.uuid) trick
            workflow.uuid = uuid;
            if (err) {
                log.error({err: err});
                if (err.name === 'UniqueAttributeError') {
                    return cb(
                      new wf.BackendInvalidArgumentError(err.message));
                } else {
                    return cb(new wf.BackendInternalError(err.message));
                }
            } else {
                if (workflow.chain) {
                    workflow.chain = JSON.parse(workflow.chain);
                }
                if (workflow.onerror) {
                    workflow.onerror = JSON.parse(workflow.onerror);
                }
                return cb(null, workflow);
            }
        });
    }

    backend.createWorkflow = createWorkflow;


    // uuid - Workflow.uuid
    // meta - Any additional information to pass to the backend which is not
    //        workflow properties
    // callback - f(err, workflow)
    function getWorkflow(uuid, meta, callback) {

        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        if (!meta.req_id) {
            meta.req_id = uuid;
        }

        var workflow = null;

        client.getObject('wf_workflows', uuid, meta, function (err, obj) {
            if (err) {
                log.error({err: err});
                if (err.name === 'ObjectNotFoundError') {
                    return callback(new wf.BackendResourceNotFoundError(sprintf(
                      'Workflow with uuid \'%s\' does not exist', uuid)));
                } else {
                    return callback(new wf.BackendInternalError(err.message));
                }
            } else {
                workflow = obj.value;
                if (workflow.chain) {
                    try {
                        workflow.chain = JSON.parse(workflow.chain);
                    } catch (e1) {
                    }
                }
                if (workflow.onerror) {
                    try {
                        workflow.onerror = JSON.parse(workflow.onerror);
                    } catch (e2) {
                    }
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
    }

    backend.getWorkflow = getWorkflow;


    // workflow - the workflow object
    // meta - Any additional information to pass to the backend which is not
    //        workflow properties
    // callback - f(err, boolean)
    function deleteWorkflow(workflow, meta, cb) {

        if (typeof (meta) === 'function') {
            cb = meta;
            meta = {};
        }

        if (!meta.req_id) {
            meta.req_id = workflow.uuid;
        }

        client.delObject('wf_workflows', workflow.uuid, meta, function (err) {
            if (err) {
                log.error({err: err});
                return cb(new wf.BackendInternalError(err.message));
            } else {
                return cb(null, true);
            }
        });
    }

    backend.deleteWorkflow = deleteWorkflow;


    // workflow - update workflow object
    // meta - Any additional information to pass to the backend which is not
    //        workflow properties
    // cb - f(err, workflow)
    function updateWorkflow(workflow, meta, cb) {

        if (typeof (meta) === 'function') {
            cb = meta;
            meta = {};
        }

        var p;
        var uuid = workflow.uuid;

        // Will use it as key, avoid trying to save it twice
        delete workflow.uuid;

        if (!meta.req_id) {
            meta.req_id = uuid;
        }

        // TODO: A good place to verify that the same tasks are not on the chain
        // and into the onerror callback (GH-1).


        for (p in workflow) {
            if (typeof (workflow[p]) === 'object') {
                workflow[p] = JSON.stringify(workflow[p]);
            }
        }

        client.getObject('wf_workflows', uuid, meta,
            function (err, aWorkflow) {
                if (err) {
                    log.error({err: err});
                    // Whatever the result, undo the delete(workflow.uuid) trick
                    workflow.uuid = uuid;
                    if (err.name === 'ObjectNotFoundError') {
                        return cb(new wf.BackendResourceNotFoundError(sprintf(
                          'Workflow with uuid \'%s\' does not exist', uuid)));
                    } else {
                        return cb(new wf.BackendInternalError(err.message));
                    }
                } else {
                    return client.putObject('wf_workflows', uuid, workflow,
                        meta, function (err) {
                            // Whatever the result, undo delete(workflow.uuid)
                            // trick
                            workflow.uuid = uuid;
                            if (err) {
                                log.error({err: err}, 'Update workflow error');
                                if (err.code === 'Invalid Argument') {
                                    return cb(
                                        new wf.BackendInvalidArgumentError(
                                            err.message));
                                } else {
                                    return cb(
                                      new wf.BackendInternalError(err.message));
                                }
                            } else {
                                if (workflow.chain) {
                                    try {
                                        workflow.chain =
                                            JSON.parse(workflow.chain);
                                    } catch (e1) {}
                                }
                                if (workflow.onerror) {
                                    try {
                                        workflow.onerror =
                                            JSON.parse(workflow.onerror);
                                    } catch (e2) {}
                                }
                                return cb(null, workflow);
                              }
                        });
                }
            });
    }

    backend.updateWorkflow = updateWorkflow;

    // job - Job object
    // meta - Any additional information to pass to the backend which is not
    //        job properties
    // callback - f(err, job)
    function createJob(job, meta, callback) {
        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowMorayBackend.createJob job(Object) required'));
        }

        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        var uuid = job.uuid;
        var p;

        if (!meta.req_id) {
            meta.req_id = uuid;
        }

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

        return _putJob(uuid, job, meta, function (err, meta, job) {
            return callback(err, job);
        });
    }

    backend.createJob = createJob;

    // uuid - Job.uuid
    // meta - Any additional information to pass to the backend which is not
    //        job properties
    // callback - f(err, job)
    function getJob(uuid, meta, callback) {
        if (typeof (uuid) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowMorayBackend.getJob uuid(String) required'));
        }

        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        if (!meta.req_id) {
            meta.req_id = uuid;
        }

        var job;
        return client.getObject('wf_jobs', uuid, meta, function (err, obj) {
            if (err) {
                log.error({err: err});
                if (err.name === 'ObjectNotFoundError') {
                    return callback(new wf.BackendResourceNotFoundError(sprintf(
                      'Job with uuid \'%s\' does not exist', uuid)));
                } else {
                    return callback(new wf.BackendInternalError(err.message));
                }
            } else {
                job = obj.value;
                job.uuid = uuid;
                return _decodeJob(job, function (j) {
                    return callback(null, j);
                });
            }
        });
    }

    backend.getJob = getJob;

    // Get a single job property
    // uuid - Job uuid.
    // prop - (String) property name
    // meta - Any additional information to pass to moray which is not
    //        job properties
    // cb - callback f(err, value)
    function getJobProperty(uuid, prop, meta, cb) {
        if (typeof (uuid) === 'undefined') {
            return cb(new wf.BackendInternalError(
                'WorkflowMorayBackend.getJobProperty uuid(String) required'));
        }
        if (typeof (prop) === 'undefined') {
            return cb(new wf.BackendInternalError(
                'WorkflowMorayBackend.getJobProperty prop(String) required'));
        }
        if (typeof (meta) === 'function') {
            cb = meta;
            meta = {};
        }

        return getJob(uuid, meta, function (err, job) {
            if (err) {
                log.error({err: err});
                return cb(err);
            } else {
                return cb(null, job[prop]);
            }
        });
    }

    backend.getJobProperty = getJobProperty;


    // job - the job object
    // callback - f(err) called with error in case there is a duplicated
    // job with the same target and same params
    function validateJobTarget(job, callback) {
        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
                'WorkflowMorayBackend.validateJobTarget job(Object) required'));
        }

        // If no target is given, we don't care:
        if (!job.target) {
            return callback(null);
        }
        var filter = '(&(target=' + job.target +
              ')(|(execution=queued)(execution=running)))';
        var req = client.findObjects('wf_jobs', filter);
        var is_dup = false;
        var onError = function (err) {
            log.error({err: err}, 'findObject error');
            req.removeAllListeners('record');
            req.removeAllListeners('end');
            return callback(new wf.BackendInternalError(err.message));
        };

        var onRecord = function (obj) {
            var val = obj.value;
            var parsed_params;
            try {
                if (val.params) {
                    parsed_params = (typeof (val.params) === 'object') ?
                    val.params : JSON.parse(val.params);
                    is_dup = _areParamsEqual(job.params, parsed_params);
                }
            } catch (e) {
                log.error(e, 'findObject exception');
                log.info({
                    job_params: job.params,
                    val_params: val.params,
                    job_params_type: typeof (job.params),
                    val_params_type: typeof (val.params)
                }, 'Params causing exception');
                req.emit('error', e);
            }
        };

        var onEnd = function () {
            if (is_dup === false) {
                return callback(null);
            } else {
                return callback(new wf.BackendInvalidArgumentError(
                    'Another job with the same target' +
                    ' and params is already queued'));
            }
        };

        req.once('error', onError);

        req.on('record', onRecord);

        return req.once('end', onEnd);
    }

    backend.validateJobTarget = validateJobTarget;

    // Get the next queued job.
    // index - Integer, optional. When given, it'll get the job at index
    //         position (when not given, it'll return the job at position zero)
    // callback - f(err, job)
    function nextJob(index, callback) {
        if (typeof (index) === 'function') {
            callback = index;
            index = 0;
        }

        var now = new Date().getTime();
        var filter = '(&(exec_after<=' + now + ')(|(execution=queued)))';
        var opts = {
            sort: {
                attribute: 'created_at',
                order: 'ASC'
            },
            limit: 1,
            offset: index
        };
        var job = null;
        var req = client.findObjects('wf_jobs', filter, opts);

        req.once('error', function (err) {
            log.error({err: err}, 'findObject error');
            return callback(new wf.BackendInternalError(err.message));
        });

        req.on('record', function (obj) {
            job = obj.value;
            job.uuid = obj.key;
        });

        return req.on('end', function () {
            if (job) {
                return _decodeJob(job, function (j) {
                    return callback(null, j);
                });
            } else {
                return callback(null, null);
            }
        });
    }

    backend.nextJob = nextJob;

    // Get the given number of queued jobs uuids.
    // - start - Integer - Position of the first job to retrieve
    // - stop - Integer - Position of the last job to retrieve, _included_
    // - callback - f(err, jobs)
    function nextJobs(start, stop, callback) {
        var filter = '(execution=queued)';
        var opts = {
            sort: {
                attribute: 'created_at',
                order: 'ASC'
            },
            limit: (stop - start) + 1,
            offset: start
        };
        var jobs = [];
        var req = client.findObjects('wf_jobs', filter, opts);

        req.once('error', function (err) {
            log.error({err: err}, 'findObject error');
            return callback(new wf.BackendInternalError(err.message));
        });

        req.on('record', function (obj) {
            jobs.push(obj.key);
        });

        return req.on('end', function () {
            if (jobs.length) {
                return callback(null, jobs);
            } else {
                return callback(null, null);
            }
        });
    }

    backend.nextJobs = nextJobs;


    // Lock a job, mark it as running by the given runner, update job status.
    // uuid - the job uuid (String)
    // runner_id - the runner identifier (String)
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function runJob(uuid, runner_id, callback) {
        if (typeof (uuid) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowMorayBackend.runJob uuid(String) required'));
        }

        var job,
            theJob,
            etag,
            meta = {
                req_id: uuid
            };

        return client.getObject('wf_jobs', uuid, meta, function (err, obj) {
            if (err) {
                log.error({err: err}, 'getObject error');
                if (err.name === 'ObjectNotFoundError') {
                    return callback(new wf.BackendResourceNotFoundError(sprintf(
                      'Job with uuid \'%s\' does not exist', uuid)));
                } else {
                    return callback(new wf.BackendInternalError(err.message));
                }
            } else {
                job = obj.value;
                etag = obj._etag;
                return _decodeJob(job, function (j) {
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
                        return _putJob(uuid, theJob, {
                            etag: etag
                        }, function (err, obj) {
                            return callback(err, theJob);
                        });
                    }
                });
            }
        });
    }

    backend.runJob = runJob;


    // Unlock the job, mark it as finished, update the status, add the results
    // for every job's task.
    // job - the job object. It'll be saved to the backend with the provided
    //       properties.
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function finishJob(job, callback) {
        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowMorayBackend.finishJob job(Object) required'));
        }

        var p;

        for (p in job) {
            if (typeof (job[p]) === 'object') {
                job[p] = JSON.stringify(job[p]);
            }
        }

        return getJob(job.uuid, function (err, aJob) {
            if (err) {
                log.error({err: err});
                return callback(err);
            } else {
                return _decodeJob(aJob, function (j) {
                    if (j.execution !== 'running') {
                        log.error({
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
                        return _putJob(uuid, job, {},
                          function (err, job) {
                            return callback(err, job);
                        });
                    }
                });
            }
        });
    }

    backend.finishJob = finishJob;


    // Update the job while it is running with information regarding progress
    // job - the job object. It'll be saved to the backend with the provided
    //       properties.
    // meta - Any additional information to pass to the backend which is not
    //        job properties
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function updateJob(job, meta, callback) {
        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowMorayBackend.updateJob job(Object) required'));
        }

        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        var p;
        var uuid = job.uuid;

        delete job.uuid;

        for (p in job) {
            if (typeof (job[p]) === 'object') {
                job[p] = JSON.stringify(job[p]);
            }
        }

        return _putJob(uuid, job, meta, function (err, job) {
            return callback(err, job);
        });
    }

    backend.updateJob = updateJob;

    // Update only the given Job property. Intendeed to prevent conflicts with
    // two sources updating the same job at the same time, but different
    // properties
    // uuid - the job's uuid
    // prop - the name of the property to update
    // val - value to assign to such property
    // meta - Any additional information to pass to the backend which is not
    //        job properties
    // callback - f(err) called with error if something fails, otherwise with
    // null.
    function updateJobProperty(uuid, prop, val, meta, callback) {
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

        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        if (typeof (val) === 'object') {
            val = JSON.stringify(val);
        }

        return getJob(uuid, meta, function (err, aJob) {
            if (err) {
                log.error({err: err});
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
                return updateJob(aJob, meta, callback);
            }
        });
    }

    backend.updateJobProperty = updateJobProperty;


    // Queue a job which has been running; i.e, due to whatever the reason,
    // re-queue the job. It'll unlock the job, update the status, add the
    // results for every finished task so far ...
    // job - the job Object. It'll be saved to the backend with the provided
    //       properties to ensure job status persistence.
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function queueJob(job, callback) {
        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowMorayBackend.queueJob job(Object) required'));
        }

        var p;

        for (p in job) {
            if (typeof (job[p]) === 'object') {
                job[p] = JSON.stringify(job[p]);
            }
        }

        return getJob(job.uuid, function (err, aJob) {
            if (err) {
                log.error({err: err});
                return callback(err);
            } else {
                return _decodeJob(aJob, function (j) {
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
                        return _putJob(uuid, job, function (err, job) {
                            return callback(err, job);
                        });
                    }
                });
            }
        });
    }

    backend.queueJob = queueJob;


    // Register a runner on the backend and report it's active:
    // - runner_id - String, unique identifier for runner.
    // - active_at - ISO String timestamp. Optional. If none is given,
    //   current time
    // - callback - f(err)
    function registerRunner(runner_id, active_at, callback) {
        if (typeof (active_at) === 'function') {
            callback = active_at;
            active_at = new Date().toISOString();
        }
        client.putObject('wf_runners', runner_id, {
            active_at: active_at,
            idle: false
        }, function (err) {
            if (err) {
                log.error({err: err});
                return callback(new wf.BackendInternalError(err));
            } else {
                return callback(null);
            }
        });
    }

    backend.registerRunner = registerRunner;


    // Report a runner remains active:
    // - runner_id - String, unique identifier for runner. Required.
    // - active_at - ISO String timestamp. Optional. If none is given,
    //   current time
    // - callback - f(err)
    function runnerActive(runner_id, active_at, callback) {
        return registerRunner(runner_id, active_at, callback);
    }

    backend.runnerActive = runnerActive;


    // Get the given runner id details
    // - runner_id - String, unique identifier for runner. Required.
    // - callback - f(err, runner)
    function getRunner(runner_id, callback) {
        var runner;
        client.getObject('wf_runners', runner_id, function (err, obj) {
            if (err) {
                log.error({err: err});
                if (err.name === 'ObjectNotFoundError') {
                    return callback(new wf.BackendResourceNotFoundError(sprintf(
                      'Runner with id \'%s\' does not exist', runner_id)));
                } else {
                    return callback(new wf.BackendInternalError(err.message));
                }
            } else {
                runner = obj.value;
                return callback(null, new Date(runner.active_at));
            }
        });
    }

    backend.getRunner = getRunner;


    // Get all the registered runners:
    // - callback - f(err, runners)
    function getRunners(callback) {

        var filter = '(active_at=*)';
        var runners = {};
        var req = client.findObjects('wf_runners', filter);

        req.once('error', function (err) {
            log.error({err: err}, 'findObject error');
            return callback(new wf.BackendInternalError(err.message));
        });

        req.on('record', function (obj) {
            var runner = obj.value;
            runners[obj.key] = new Date(runner.active_at);
        });

        return req.on('end', function () {
            return callback(null, runners);
        });
    }

    backend.getRunners = getRunners;


    // Set a runner as idle:
    // - runner_id - String, unique identifier for runner
    // - callback - f(err)
    function idleRunner(runner_id, callback) {
        var runner;
        client.getObject('wf_runners', runner_id, function (err, obj) {
            if (err) {
                log.error({err: err});
                if (err.name === 'ObjectNotFoundError') {
                    return callback(new wf.BackendResourceNotFoundError(sprintf(
                      'Runner with id \'%s\' does not exist', runner_id)));
                } else {
                    return callback(new wf.BackendInternalError(err.message));
                }
            } else {
                runner = obj.value;
                return client.putObject('wf_runners', runner_id, {
                    active_at: runner.active_at,
                    idle: true
                }, function (err) {
                        if (err) {
                            log.error({err: err});
                            return callback(new wf.BackendInternalError(err));
                        } else {
                            return callback(null);
                        }
                });
            }
        });
    }

    backend.idleRunner = idleRunner;


    // Check if the given runner is idle
    // - runner_id - String, unique identifier for runner
    // - callback - f(boolean)
    function isRunnerIdle(runner_id, callback) {
        client.getObject('wf_runners', runner_id, function (err, obj) {
            var runner = obj.value;
            return callback((err || runner.idle));
        });
    }

    backend.isRunnerIdle = isRunnerIdle;


    // Remove idleness of the given runner
    // - runner_id - String, unique identifier for runner
    // - callback - f(err)
    function wakeUpRunner(runner_id, callback) {
        return registerRunner(runner_id, callback);
    }

    backend.wakeUpRunner = wakeUpRunner;


    // Get all jobs associated with the given runner_id
    // - runner_id - String, unique identifier for runner
    // - callback - f(err, jobs). `jobs` is an array of job's UUIDs.
    //   Note `jobs` will be an array, even when empty.
    function getRunnerJobs(runner_id, callback) {
        var filter = '(runner_id=' + runner_id + ')';
        var opts = {
            sort: {
                attribute: 'created_at',
                order: 'ASC'
            }
        };
        var jobs = [];
        var req = client.findObjects('wf_jobs', filter, opts);

        req.once('error', function (err) {
            log.error({err: err}, 'findObject error');
            return callback(new wf.BackendInternalError(err.message));
        });

        req.on('record', function (obj) {
            jobs.push(obj.key);
        });

        return req.on('end', function () {
            return callback(null, jobs);
        });
    }

    backend.getRunnerJobs = getRunnerJobs;


    // Get all the workflows:
    // - callback - f(err, workflows)
    function getWorkflows(callback) {
        var filter = '(name=*)';
        var workflows = [];
        var req = client.findObjects('wf_workflows', filter);

        req.once('error', function (err) {
            log.error({err: err}, 'findObject error');
            return callback(new wf.BackendInternalError(err.message));
        });

        req.on('record', function (obj) {
            var workflow = obj.value;
            if (workflow.chain) {
                workflow.chain = JSON.parse(workflow.chain);
            }
            if (workflow.onerror) {
                workflow.onerror = JSON.parse(workflow.onerror);
            }
            workflow.uuid = obj.key;
            workflows.push(workflow);
        });

        return req.on('end', function () {
            return callback(null, workflows);
        });
    }

    backend.getWorkflows = getWorkflows;


    // Get all the jobs:
    // - params - JSON Object. Can include the value of the job's "execution"
    //   status, and any other key/value pair to search for into job's params.
    //   - execution - String, the execution status for the jobs to return.
    //                 Return all jobs if no execution status is given.
    //   - limit - Integer, max number of Jobs to retrieve. By default 1000,
    //             which is moray's default.
    //   - offset - Integer, start retrieving Jobs from the given one. Note
    //              jobs are sorted by "created_at" DESCending.
    // - callback - f(err, jobs)
    function getJobs(params, callback) {
        var executions = [
            'queued', 'failed',
            'succeeded', 'canceled',
            'running', 'retried'
        ];
        var jobs = [];
        var req;
        var filter;
        var execution;
        var offset;
        var limit;


        if (typeof (params) === 'object') {
            execution = params.execution;
            delete params.execution;
            offset = params.offset;
            delete params.offset;
            limit = params.limit;
            delete params.limit;
        }

        if (typeof (params) === 'function') {
            callback = params;
        }

        // Just a presence filter for execution will do the trick
        if (typeof (execution) === 'undefined') {
            execution = '*';
        } else if (executions.indexOf(execution) === -1) {
            return callback(new wf.BackendInvalidArgumentError(
              'excution is required and must be one of' +
              '"queued", "failed", "succeeded", "canceled", "running"' +
              '"retried"'));
        }

        filter = '(execution=' + execution + ')';
        req = client.findObjects('wf_jobs', filter, {
            sort: {
                attribute: 'created_at',
                order: 'DESC'
            },
            limit: limit || 1000,
            offset: offset || 0
        });

        req.once('error', function (err) {
            log.error({err: err}, 'findObject error');
            return callback(new wf.BackendInternalError(err.message));
        });

        req.on('record', function (obj) {
            var job = obj.value;
            job.uuid = obj.key;
            _decodeJob(job, function (j) {
                var matches = true;
                if (typeof (params) === 'object' &&
                        Object.keys(params).length > 0) {
                    Object.keys(params).forEach(function (k) {
                        if (!j.params[k] || j.params[k] !== params[k]) {
                            matches = false;
                        }
                    });

                }

                if (matches === true) {
                    jobs.push(j);
                }
            });
        });

        return req.on('end', function () {
            return callback(null, jobs);
        });
    }

    backend.getJobs = getJobs;


    // Add progress information to an existing job:
    // - uuid - String, the Job's UUID.
    // - info - Object, {'key' => 'Value'}
    // - meta - Any additional information to pass to the backend which is not
    //        job info
    // - callback - f(err)
    function addInfo(uuid, info, meta, callback) {

        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        if (!meta.req_id) {
            meta.req_id = uuid;
        }

        client.getObject('wf_jobs', uuid, meta, function (err, obj) {
            if (err) {
                log.error({err: err});
                if (err.name === 'ObjectNotFoundError') {
                    return callback(new wf.BackendResourceNotFoundError(sprintf(
                      'Job with uuid \'%s\' does not exist', uuid)));
                } else {
                    return callback(new wf.BackendInternalError(err.message));
                }
            } else {
                return client.putObject('wf_jobs_info', node_uuid(), {
                    job_uuid: uuid,
                    created_at: new Date().getTime(),
                    info: JSON.stringify(info)
                }, meta, function (err) {
                    if (err) {
                        log.error({err: err});
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
    }

    backend.addInfo = addInfo;


    // Get progress information from an existing job:
    // - uuid - String, the Job's UUID.
    // - meta - Any additional information to pass to the backend which is not
    //        job info
    // - callback - f(err, info)
    function getInfo(uuid, meta, callback) {
        var filter = '(job_uuid=' + uuid + ')';
        var opts = {
            sort: {
                attribute: 'created_at',
                order: 'ASC'
            }
        };
        var info = [];

        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        return getJob(uuid, meta, function (err, job) {
            if (err) {
                return callback(err);
            } else {
                var req = client.findObjects('wf_jobs_info', filter, opts);
                req.once('error', function (err) {
                    log.error({err: err}, 'findObject error');
                    return callback(new wf.BackendInternalError(err.message));
                });

                req.on('record', function (obj) {
                    var i = obj.value;
                    if (typeof (i.info) === 'string') {
                        i.info = JSON.parse(i.info);
                    }
                    info.push(i.info);
                });

                return req.on('end', function () {
                    return callback(null, info);
                });
            }
        });
    }

    backend.getInfo = getInfo;

    return backend;
};
