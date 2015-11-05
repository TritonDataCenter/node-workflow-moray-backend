/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');
var vasync = require('vasync');
var Logger = require('bunyan');
var wf = require('wf');
var node_uuid = require('node-uuid');
var makeEmitter = wf.makeEmitter;
var sprintf = util.format;

var bu = require('./wf-buckets-definition');

// Returns true when "obj" (Object) has all the properties "kv" (Object) has,
// and with exactly the same values, otherwise, false
function hasPropsAndVals(obj, kv) {
    if (typeof (obj) !== 'object' || typeof (kv) !== 'object') {
        return (false);
    }

    if (Object.keys(kv).length === 0) {
        return (true);
    }

    return (Object.keys(kv).every(function (k) {
        return (obj[k] && obj[k] === kv[k]);
    }));
}

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

    var definitions = bu.definitions;
    var buckets = [];

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
            }, function (e) {
                if (e) {
                    log.error({err: e}, 'Ping moray client error');
                }
                callback(e);
            });
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
                return callback(false);
            }
            return callback(bucket.options.version === config.version);
        });
    }

    backend._bucketExists = _bucketExists;

    function _createBucket(name, next) {
        return _bucketExists(name, function (exists) {
            if (!exists) {
                buckets[name].options = {
                    version: config.version
                };
                return client.putBucket(name, buckets[name], function (err) {
                    if (err) {
                        var msg = sprintf('Error creating bucket %s.', name);
                        if (err.name === 'BucketVersionError') {
                            msg = msg +
                            ' Cannot update a bucket to an older version';
                        }
                        log.error({err: err}, msg);
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
    // Private: Create wf_workflows bucket if not exists:
    function _createWfWorkflowsBucket(_, next) {
        _createBucket('wf_workflows', next);
    }


    // Private: create wf_jobs bucket if not exists:
    function _createWfJobsBucket(_, next) {
        _createBucket('wf_jobs', next);
    }

    // Private: create wf_runners bucket if not exists:
    function _createWfRunnersBucket(_, next) {
        _createBucket('wf_runners', next);
    }

    // Private: create wf_jobs_info bucket if not exists:
    function _createWfJobsInfoBucket(_, next) {
        _createBucket('wf_jobs_info', next);
    }

    // Private: create wf_locked_targets bucket if not exists:
    function _createWfLockedTargetsBucket(_, next) {
        _createBucket('wf_locked_targets', next);
    }

    function _initBucketsDefinitions(_, next) {
        definitions = bu.setup(definitions, config.extra_fields);
        Object.keys(definitions).forEach(function (bucket) {
            buckets[bucket] = bu._2morayBucket(definitions[bucket]);
        });
        return next(null, true);
    }
    // Private
    // Create all the moray buckets required by the module only if they don't
    // exist
    // callback - f(err)
    function _createBuckets(callback) {
        var funcs = [
            _initBucketsDefinitions,
            _createWfWorkflowsBucket,
            _createWfJobsBucket,
            _createWfRunnersBucket,
            _createWfJobsInfoBucket,
            _createWfLockedTargetsBucket
        ];

        vasync.pipeline({
            funcs: funcs
        }, function (err, results) {
            if (err) {
                log.error({err: err}, 'pipeline error');
                if (callback) {
                    return callback(err);
                } else {
                    return false;
                }
            } else {
                if (callback) {
                    return callback(null);
                } else {
                    return false;
                }
            }
        });
    }

    backend._createBuckets = _createBuckets;


    // Private.
    // Return all the JSON.stringified job properties decoded back to objects
    // - job - (object) raw job from moray to decode
    // - chain - (boolean) return or not workflow chain included with the job
    //   (optional, false by default)
    // - callback - (function) f(job)
    function _decodeJob(job, chain, callback) {
        if (typeof (chain) === 'function') {
            callback = chain;
            chain = false;
        }

        job = definitions.wf_jobs.decode(job, definitions.wf_jobs.fields);
        if (chain === false) {
            delete job.chain;
            delete job.onerror;
            delete job.oncancel;
        }

        Object.keys(job).forEach(function (k) {
            if (job[k] === 'null') {
                job[k] = null;
            }
        });

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


    // Private. Validate Job before saving
    // job - Object, job to validat
    // callback - f(err)
    function _validateJob(job, callback) {
        vasync.pipeline({
            'funcs': [
                function requiredFields(_, cb) {
                    var errors = [];
                    ['chain', 'workflow_uuid'].forEach(
                        function (field) {
                            if (!job[field]) {
                                errors.push(field + ' is required');
                            }
                        });

                    if (errors.length) {
                        return cb(new wf.BackendMissingParameterError(
                                errors.join(', ')));
                    }

                    if (!job.created_at) {
                        job.created_at = new Date().getTime();
                    }
                    if (!job.exec_after) {
                        job.exec_after =
                            new Date().getTime();
                    }

                    if (!job.execution) {
                        job.execution = 'queued';
                    }
                    return cb();
                },
                function validateExecution(_, cb) {
                    var exec = job.execution,
                        statuses = ['queued',
                                    'failed',
                                    'succeeded',
                                    'canceled',
                                    'running',
                                    'retried',
                                    'waiting'];
                    if (!exec || statuses.indexOf(exec) !== -1) {
                        return cb();
                    } else {
                        return cb(
                          new wf.BackendInvalidArgumentError(
                              'execution is invalid'));
                    }
                },
                function validateUUID(_, cb) {
                    var UUID_FORMAT = new RegExp('^' + [
                        '[0-9a-f]{8}',
                        '[0-9a-f]{4}',
                        '4[0-9a-f]{3}',
                        '[89ab][0-9a-f]{3}',
                        '[0-9a-f]{12}'
                    ].join('-') + '$', 'i');
                    var wf_uuid = job.workflow_uuid;

                    if (!wf_uuid || wf_uuid.match(UUID_FORMAT)) {
                        return cb(null);
                    } else {
                        return cb(new wf.BackendInvalidArgumentError(
                              'workflow_uuid is invalid'));
                    }
                },
                function checkValidDates(_, cb) {
                    var errors = [];
                    var dates = ['exec_after', 'created_at'];
                    var util = require('util');
                    dates.forEach(function (d) {
                        var day = new Date(job[d]);
                        if (!util.isDate(day)) {
                            errors.push(d + ' is invalid');
                        }
                    });
                    if (errors.length) {
                        return cb(new wf.BackendInvalidArgumentError(
                                    errors.join(', ')));
                    }
                    return cb(null);
                }
            ]
        }, function (err, results) {
            if (err) {
                return callback(err);
            }
            return callback(null);
        });
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

        job = definitions.wf_jobs.encode(job);

        if (!opts.req_id) {
            opts.req_id = uuid;
        }

        _validateJob(job, function (err) {
            if (err) {
                return callback(err);
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
                            return callback(
                                new wf.BackendPreconditionFailedError(
                              'Job has been locked by another runner'));
                        } else {
                            return callback(
                                new wf.BackendInternalError(err.message));
                        }
                    } else {
                        return _decodeJob(job, true, function (j) {
                            callback(null, j);
                        });
                    }
                });

            } catch (e) {
                log.error(e, 'Exception trying to put Job to Moray');
                return callback(new wf.BackendInternalError(e.message));
            }

        });
    }


    // Attempt to connect the client to the backend.
    // On success, create required buckets when necessary.
    function init(createBuckets, callback) {
        if (typeof (createBuckets) === 'function') {
            callback = createBuckets;
            createBuckets = true;
        }
        var url = config.url || 'http://127.0.0.1:2020';
        var moray = require('moray');
        var backoff = require('backoff');

        var connected = false;

        function makeClient() {
            var _client = moray.createClient({
                checkInterval: 10 * 1000,
                connectTimeout: config.connectTimeout,
                url: url,
                log: log,
                noCache: true,
                reconnect: true,
                maxConnections: 10,
                maxIdleTime: 600 * 1000,
                pingTimeout: 4000,
                retry: { // try to reconnect forever
                    maxTimeout: 2000,
                    minTimeout: 100,
                    retries: Infinity
                }
            });
            return (_client);
        }

        function connectCb() {
            backend.client = client;
            backend.connected = true;
            backend.emit('connected');
            if (createBuckets) {
                if (callback) {
                    return _createBuckets(callback);
                }
                return _createBuckets();
            } else {
                return (callback) ? callback() : true;
            }
        }

        function errorCb(err) {
            log.error({
                err: err
            }, 'Moray client error');
            backend.connected = false;
            backend.emit('error', err);
            backend.init(createBuckets, callback);
        }

        var retry = backoff.exponential({
            maxTimeout: 30000,
            maxDelay: Infinity
        });

        // retry operation about to get started with the given delay:
        retry.on('backoff', function (number, delay, err) {
            log.info({
                attempt: number.toString(),
                delay: delay,
                err: err
            }, 'ring: moray connection attempted: %s', number.toString());
        });

        // retry complete, check if we're connected, fire another attempt
        // either if we are not connected or ping request fails:
        retry.on('ready', function (number, delay) {
            if (typeof (number) !== 'undefined') {
                log.info({
                    attempt: number,
                    delay: delay
                }, 'ring: moray connection attempt done');
            }

            function onError(err) {
                log.error({err: err}, 'Moray connection error');
            }

            function onConnect() {
                log.info('Moray client connected, sending ping request.');

                // Assuming we're connected here, let's ping the client:
                client.ping({
                    log: log
                }, function (err) {
                    if (err) {
                        log.error({
                            err: err
                        }, 'Ping moray error, retrying connection');
                    } else {
                        connected = true;
                        client.removeListener('connect', onConnect);
                    }
                });
            }

            if (connected) {
                log.info('Successfully connected to moray');
                // Do real stuff here:
                client.removeListener('error', onError);
                client.on('error', errorCb);
                retry.reset();
                connectCb();
            } else {
                client = makeClient();
                client.on('connect', onConnect);
                client.on('error', onError);
                retry.backoff();
            }
        });

        // Init the connecte retries:
        retry.emit('ready');

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

        var uuid = workflow.uuid;

        if (!meta.req_id) {
            meta.req_id = uuid;
        }

        meta.etag = null;

        // Will use it as key, avoid trying to save it twice
        delete workflow.uuid;

        workflow = definitions.wf_workflows.encode(workflow);

        client.putObject('wf_workflows', uuid, workflow, meta, function (err) {
            // Whatever the result, undo the delete(workflow.uuid) trick
            workflow.uuid = uuid;
            if (err) {
                log.error({err: err}, 'createWorkflow error');
                if (err.name === 'UniqueAttributeError') {
                    return cb(
                      new wf.BackendInvalidArgumentError(err.message));
                } else {
                    return cb(new wf.BackendInternalError(err.message));
                }
            } else {
                workflow = definitions.wf_workflows.decode(workflow,
                    definitions.wf_workflows.fields);
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
                if (err.name === 'ObjectNotFoundError') {
                    return callback(new wf.BackendResourceNotFoundError(sprintf(
                      'Workflow with uuid \'%s\' does not exist', uuid)));
                } else {
                    log.error({err: err}, 'getWorkflow Error');
                    return callback(new wf.BackendInternalError(err.message));
                }
            } else {
                workflow = obj.value;
                workflow = definitions.wf_workflows.decode(workflow,
                    definitions.wf_workflows.fields);
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
                log.error({err: err}, 'deleteWorkflow error');
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

        var uuid = workflow.uuid;

        // Will use it as key, avoid trying to save it twice
        delete workflow.uuid;

        if (!meta.req_id) {
            meta.req_id = uuid;
        }

        workflow = definitions.wf_workflows.encode(workflow);

        client.getObject('wf_workflows', uuid, meta,
            function (err, aWorkflow) {
                if (err) {
                    log.error({err: err}, 'updateWorkflow error');
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
                                workflow = definitions.wf_workflows.decode(
                                    workflow, definitions.wf_workflows.fields);
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
        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowMorayBackend.createJob job(Object) required'));
        }

        var uuid = job.uuid;

        if (!meta.req_id) {
            meta.req_id = uuid;
        }
        meta.etag = null;

        delete job.uuid;

        job.created_at = (job.created_at) ?
              new Date(job.created_at).getTime() : new Date().getTime();
        job.exec_after = (job.exec_after) ?
              new Date(job.exec_after).getTime() : new Date().getTime();
        job.execution = job.execution || 'queued';

        return _putJob(uuid, job, meta, function (err, job, meta) {
            if (err) {
                return callback(err);
            }
            if (typeof (job.locks) !== 'undefined') {
                return client.putObject('wf_locked_targets', job.uuid, {
                    target: job.locks
                }, function (err1) {
                    return callback(err1, job);
                });
            }
            return callback(null, job);
        });
    }

    backend.createJob = createJob;

    // uuid - Job.uuid
    // meta - Any additional information to pass to the backend which is not
    //        job properties
    // callback - f(err, job)
    function getJob(uuid, meta, callback) {
        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        if (typeof (uuid) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowMorayBackend.getJob uuid(String) required'));
        }

        if (!meta.req_id) {
            meta.req_id = uuid;
        }

        var job;
        return client.getObject('wf_jobs', uuid, meta, function (err, obj) {
            if (err) {
                if (err.name === 'ObjectNotFoundError') {
                    return callback(new wf.BackendResourceNotFoundError(sprintf(
                      'Job with uuid \'%s\' does not exist', uuid)));
                } else {
                    log.error({err: err}, 'getJob Error');
                    return callback(new wf.BackendInternalError(err.message));
                }
            } else {
                job = obj.value;
                job.uuid = uuid;
                return _decodeJob(job, true, function (j) {
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
        if (typeof (meta) === 'function') {
            cb = meta;
            meta = {};
        }

        if (typeof (uuid) === 'undefined') {
            return cb(new wf.BackendInternalError(
                'WorkflowMorayBackend.getJobProperty uuid(String) required'));
        }
        if (typeof (prop) === 'undefined') {
            return cb(new wf.BackendInternalError(
                'WorkflowMorayBackend.getJobProperty prop(String) required'));
        }

        return getJob(uuid, meta, function (err, job) {
            if (err) {
                log.error({err: err}, 'getJobProperty error');
                return cb(err);
            } else {
                return cb(null, job[prop]);
            }
        });
    }

    backend.getJobProperty = getJobProperty;

    // Private: Check if the given job target is locked
    function _isTargetLocked(target, cb) {
        var res = [];

        var req = client.findObjects('wf_locked_targets', '(target=*)');

        req.once('error', function (err) {
            return cb(err);
        });

        req.on('record', function (obj) {
            // TODO: Could try to match as soon as we get a target and
            // remove listeners + return from here if target matches
            // (maybe could have a race condition on such approach?)
            res.push(obj.value.target);
        });

        req.once('end', function () {
            var locked = res.some(function (r) {
                var re = new RegExp(r);
                return (re.test(target));
            });
            return cb(null, locked);
        });
    }

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
        return _isTargetLocked(job.target, function (err1, locked) {
            if (err1) {
                return callback(new wf.BackendInternalError(err1.message));
            }

            if (locked) {
                return callback(new wf.BackendInvalidArgumentError(
                    'Job target is currently locked by another job'));
            }

            var filter = '(&(target=' + job.target +
                  ')(|(execution=queued)(execution=running)' +
                  '(execution=waiting)))';
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
        });
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

        return req.once('end', function () {
            if (job) {
                return _decodeJob(job, true, function (j) {
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

        return req.once('end', function () {
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
                return _decodeJob(job, true, function (j) {
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

    // Internal method used by finish/resume/pause/queue Job methods
    function _processJob(job, func, from_status, to_status, callback) {
        if (typeof (job) === 'undefined') {
            return callback(
                    new wf.BackendInternalError(
                    sprintf('WorkflowMorayBackend.%s job(Object) required',
                        func)));
        }

        return getJob(job.uuid, function (err, aJob) {
            if (err) {
                log.error({err: err}, 'processJob error');
                return callback(err);
            } else {
                return _decodeJob(aJob, true, function (j) {
                    if (from_status !== null && j.execution !== from_status) {
                        return callback(new wf.BackendPreconditionFailedError(
                          sprintf(
                              'Only %s jobs can be %s. Job execution is: %s',
                              from_status, to_status, job.execution)));
                    } else {
                        var uuid = job.uuid;
                        delete job.uuid;
                        job.execution = to_status;
                        delete job.runner_id;
                        return _putJob(uuid, job, function (err, theJob) {
                            return callback(err, theJob);
                        });
                    }
                });
            }
        });
    }


    // Unlock the job, mark it as finished, update the status, add the results
    // for every job's task.
    // job - the job object. It'll be saved to the backend with the provided
    //       properties.
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function finishJob(job, callback) {
        var to_status = (job.execution === 'running') ?
            'succeeded' : job.execution;
        return _processJob(job, 'finishJob', null, to_status,
            function (err, theJob) {
                if (err) {
                    return callback(err);
                }
                if (typeof (theJob.locks) !== 'undefined') {
                    client.delObject('wf_locked_targets', theJob.uuid,
                        function (err1) {
                            if (err1) {
                                return callback(err1, theJob);
                            }
                            return callback(err, theJob);
                        });
                }
                return callback(err, theJob);
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
        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

        if (typeof (job) === 'undefined') {
            return callback(new wf.BackendInternalError(
                  'WorkflowMorayBackend.updateJob job(Object) required'));
        }

        var uuid = job.uuid;

        delete job.uuid;

        return _putJob(uuid, job, meta, function (err, theJob) {
            return callback(err, theJob);
        });
    }

    backend.updateJob = updateJob;


    // Unlock the job, mark it as canceled, and remove the runner_id
    // uuid - string, the job uuid.
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function cancelJob(uuid, callback) {
        if (typeof (uuid) === 'undefined') {
            return callback(new wf.BackendInternalError(
              'WorkflowMorayBackend.cancelJob uuid(String) required'));
        }

        return getJob(uuid, function (err, aJob) {
            if (err) {
                log.error({err: err}, 'cancelJob error');
                return callback(err);
            } else {
                aJob.execution = 'canceled';
                delete aJob.runner_id;
                return updateJob(aJob, function (err2, job) {
                    if (err2) {
                        return callback(err2);
                    }
                    if (job && typeof (job.locks) !== 'undefined') {
                        return client.delObject('wf_locked_targets', job.uuid,
                            function (err1) {
                                if (err1) {
                                    return callback(err1, job);
                                }
                                return callback(null, job);
                            });
                    } else {
                        return callback(null, job);
                    }
                });
            }
        });
    }

    backend.cancelJob = cancelJob;


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
        if (typeof (meta) === 'function') {
            callback = meta;
            meta = {};
        }

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

        return getJob(uuid, meta, function (err, aJob) {
            if (err) {
                log.error({err: err}, 'updateJobProperty error');
                return callback(err);
            } else {
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
        return _processJob(job, 'queueJob', 'running', 'queued', callback);
    }

    backend.queueJob = queueJob;

    // Pause a job which has been running; i.e, due to whatever the reason,
    // change the status to waiting. It'll unlock the job, update the status,
    // add the results for every finished task so far ...
    // job - the job Object. It'll be saved to the backend with the provided
    //       properties to ensure job status persistence.
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function pauseJob(job, callback) {
        return _processJob(job, 'pauseJob', 'running', 'waiting', callback);
    }

    backend.pauseJob = pauseJob;


    // Resumes a job which has been paused.
    // change the status to queued. It'll unlock the job, update the status.
    //  job - the job Object. It'll be saved to the backend with the provided
    //       properties to ensure job status persistence.
    // callback - f(err, job) callback will be called with error if something
    //            fails, otherwise it'll return the updated job using getJob.
    function resumeJob(job, callback) {
        return _processJob(job, 'resumeJob', 'waiting', 'queued', callback);
    }

    backend.resumeJob = resumeJob;


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
                log.error({err: err}, 'registerRunner error');
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
        client.getObject('wf_runners', runner_id, function (err, obj) {
            var runner;
            if (err) {
                log.error({err: err}, 'getRunner error');
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
            log.error({err: err}, 'getRunners findObject error');
            return callback(new wf.BackendInternalError(err.message));
        });

        req.on('record', function (obj) {
            var runner = obj.value;
            runners[obj.key] = new Date(runner.active_at);
        });

        return req.once('end', function () {
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
                log.error({err: err}, 'idleRunner error');
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
                            log.error({err: err}, 'idleRunner putObject error');
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
            if (err) {
                callback(err);
            } else {
                callback(obj.value.idle);
            }
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

        return req.once('end', function () {
            return callback(null, jobs);
        });
    }

    backend.getRunnerJobs = getRunnerJobs;


    // Get all the workflows:
    // - params - JSON Object (Optional). Can include the value of the
    //  workflow's "name", and any other key/value pair to search for
    //   into workflow's definition.
    // - callback - f(err, workflows)
    function getWorkflows(params, callback) {

        if (typeof (params) === 'function') {
            callback = params;
            params = {};
        }

        var filter = '(name=*)';
        if (params.name) {
            filter = '(name=' + params.name + ')';
            delete params.name;
        }

        var workflows = [];
        var req = client.findObjects('wf_workflows', filter);

        req.once('error', function (err) {
            log.error({err: err}, 'findObject error');
            return callback(new wf.BackendInternalError(err.message));
        });

        req.on('record', function (obj) {
            var workflow = obj.value;
            workflow = definitions.wf_workflows.decode(workflow,
                    definitions.wf_workflows.fields);
            workflow.uuid = obj.key;

            if (hasPropsAndVals(workflow, params)) {
                workflows.push(workflow);
            }
        });

        return req.once('end', function () {
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
    // - callback - f(err, jobs, count)
    function getJobs(params, chain, callback) {

        var executions = [
            'queued', 'failed',
            'succeeded', 'canceled',
            'running', 'retried',
            'waiting'
        ];
        var jobs = [];
        var req;
        var filter;
        var pieces = [];
        var execution;
        var offset;
        var limit;
        var target;
        var extraFields = {};

        // Special params to find Jobs created_at since, until and between a
        // given time using job.created_at
        var since;
        var until;
        // Return the total number of Jobs matching the given filter for those
        // cases where we're limiting the search
        var count = 0;


        if (typeof (params) === 'function') {
            callback = params;
            params = {};
            chain = false;
        } else if (typeof (chain) === 'function') {
            callback = chain;
            chain = false;
        }

        if (typeof (params) === 'object' && Object.keys(params).length) {
            var knownParams = ['execution', 'offset', 'limit',
                'since', 'until', 'target', 'name'];
            var configurableParams = Object.keys(config.extra_fields.wf_jobs);
            knownParams = knownParams.concat(configurableParams);

            var invalidParams = [];

            Object.keys(params).forEach(function (p) {
                if (knownParams.indexOf(p) === -1) {
                    invalidParams.push(p);
                }
            });

            if (invalidParams.length) {
                log.error({invalidParams: invalidParams}, 'getJobs error');
                return callback(new wf.BackendInvalidArgumentError(
                    'Invalid parameters for jobs search: ' +
                    invalidParams.join(',')));
            }

            execution = params.execution;
            delete params.execution;
            offset = params.offset;
            delete params.offset;
            limit = params.limit;
            delete params.limit;

            since = params.since;
            delete params.since;
            until = params.until;
            delete params.until;
            target = params.target;
            delete params.target;

            configurableParams.forEach(function (k) {
                if (params[k]) {
                    extraFields[k] = params[k];
                    delete params[k];
                }
            });
        }

        // 'since' and 'until' could take an String Date representation
        if (since && isNaN(Number(since))) {
            try {
                since = new Date(since).getTime();
            } catch (e1) {
                since = null;
            }
        }

        if (until && isNaN(Number(until))) {
            try {
                until = new Date(until).getTime();
            } catch (e2) {
                until = null;
            }
        }

        // Just a presence filter for execution will do the trick
        if (typeof (execution) === 'undefined') {
            execution = '*';
        } else if (executions.indexOf(execution) === -1) {
            return callback(new wf.BackendInvalidArgumentError(
              'excution is required and must be one of' +
              '"queued", "failed", "succeeded", "canceled", "running"' +
              '"retried", "waiting"'));
        }

        pieces.push('(execution=' + execution + ')');

        if (params.name) {
            pieces.push('(name=' + params.name + ')');
            delete params.name;
        }

        if (typeof (target) !== 'undefined') {
            pieces.push('(target=' + target + ')');
        }

        if (typeof (since) !== 'undefined') {
            pieces.push('(created_at>=' + since + ')');
        }

        if (typeof (until) !== 'undefined') {
            pieces.push('(created_at<=' + until + ')');
        }

        Object.keys(extraFields).forEach(function (k) {
            pieces.push('(' + k + '=' + extraFields[k] + ')');
        });

        filter = '(&' + pieces.join('') + ')';

        log.info({filter: filter}, 'getJobs filter');

        req = client.findObjects('wf_jobs', filter, {
            sort: {
                attribute: 'created_at',
                order: 'DESC'
            },
            limit: limit || 1000,
            offset: offset || 0
        });

        req.once('error', function (err) {
            log.error({err: err}, 'findObject error');
            return callback(new wf.BackendInternalError(err.message));
        });

        req.on('record', function (obj) {
            if (count === 0) {
                count = Number(obj._count);
            }
            var job = obj.value;
            job.uuid = obj.key;
            _decodeJob(job, chain, function (j) {
                if (hasPropsAndVals(j.params, params)) {
                    jobs.push(j);
                }
            });
        });

        return req.once('end', function () {
            return callback(null, jobs, count);
        });
    }

    backend.getJobs = getJobs;


    function countJobs(callback) {
        var executions = [
            'queued', 'failed',
            'succeeded', 'canceled',
            'running', 'retried',
            'waiting'
        ];

        var yesterday = (function (d) {
            d.setDate(d.getDate() - 1);
            return d;
        })(new Date()).getTime();

        var _1hr = (function (d) {
            d.setHours(d.getHours() - 1);
            return d;
        })(new Date()).getTime();

        var _2hr = (function (d) {
            d.setHours(d.getHours() - 2);
            return d;
        })(new Date()).getTime();

        var q = 'select count(_id), execution from wf_jobs where %s group' +
            ' by execution;';

        var stats = [ {
            name: 'all_time',
            query: sprintf(q, ('created_at < ' + yesterday))
        }, {
            name: 'past_24h',
            query: sprintf(q, ('created_at > ' + yesterday +
                        ' AND created_at < ' + _1hr))
        }, {
            name: 'past_hour',
            query: sprintf(q, ('created_at > ' + _2hr +
                        ' AND created_at < ' + _1hr))
        }, { name: 'current',
            query: sprintf(q, ('created_at > ' + _1hr))
        }];

        vasync.forEachParallel({
            inputs: stats,
            func: function _countJobs(stat, cb) {
                stat.results = {};
                var req = client.sql(stat.query);

                req.once('error', function (err) {
                    log.error({err: err}, 'Count jobs error');
                    cb(new wf.BackendInternalError(err.message));
                });

                req.on('record', function (obj) {
                    if (obj) {
                        if (obj.count) {
                            obj.count = parseInt(obj.count, 10);
                        }
                        stat.results[obj.execution] = obj.count;
                    }
                });

                req.once('end', function () {
                    cb(null, stat);
                });
            }
        }, function (err, results) {
            if (err) {
                return callback(err);
            } else {
                var out = {};
                results.successes.forEach(function (r) {
                    executions.forEach(function (e) {
                        if (!r.results[e]) {
                            r.results[e] = 0;
                        }
                    });
                    out[r.name] = r.results;
                });
                return callback(null, out);
            }
        });
    }

    backend.countJobs = countJobs;


    // Private. Validate job info:
    function _validateJobInfo(info, callback) {
        vasync.pipeline({
            'funcs': [
                function requiredInfoFields(_, cb) {
                    var errors = [];
                    ['info', 'job_uuid'].forEach(function (field) {
                        if (!info[field]) {
                            errors.push(field + ' is required');
                        }
                    });

                    if (errors.length) {
                        return cb(new wf.BackendMissingParameterError(
                                errors.join(', ')));
                    }
                    if (!info.created_at) {
                        info.created_at = new Date().getTime();
                    }
                    return cb();
                },
                function validateJobsInfoUUID(_, cb) {
                    var UUID_FORMAT = new RegExp('^' + [
                        '[0-9a-f]{8}',
                        '[0-9a-f]{4}',
                        '4[0-9a-f]{3}',
                        '[89ab][0-9a-f]{3}',
                        '[0-9a-f]{12}'
                    ].join('-') + '$', 'i'),
                    job_uuid = info.job_uuid;

                    if (!job_uuid || job_uuid.match(UUID_FORMAT)) {
                        return cb(null);
                    } else {
                        return cb(new wf.BackendInvalidArgumentError(
                                    'job_uuid is invalid'));
                    }
                },
                function checkJobsInfoValidDates(_, cb) {
                    var util = require('util');
                    var day = new Date(info.created_at);
                    if (!util.isDate(day)) {
                        return cb(
                          new wf.BackendInvalidArgumentError(
                              info.created_at + ' is invalid'));
                    } else {
                        return cb(null);
                    }
                }
            ]
        }, function (err, results) {
            if (err) {
                return callback(err);
            }
            return callback(null);
        });
    }
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
                log.error({err: err}, 'addInfo error');
                if (err.name === 'ObjectNotFoundError') {
                    return callback(new wf.BackendResourceNotFoundError(sprintf(
                      'Job with uuid \'%s\' does not exist', uuid)));
                } else {
                    return callback(new wf.BackendInternalError(err.message));
                }
            } else {
                var o = {
                    job_uuid: uuid,
                    created_at: new Date().getTime(),
                    info: info
                };
                o = definitions.wf_jobs_info.encode(o);

                return client.putObject('wf_jobs_info', node_uuid(), o, meta,
                    function (err) {
                    if (err) {
                        log.error({err: err}, 'addInfo error');
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
                    i = definitions.wf_jobs_info.decode(i,
                    definitions.wf_jobs_info.fields);
                    info.push(i.info);
                });

                return req.once('end', function () {
                    return callback(null, info);
                });
            }
        });
    }

    backend.getInfo = getInfo;

    return backend;
};
