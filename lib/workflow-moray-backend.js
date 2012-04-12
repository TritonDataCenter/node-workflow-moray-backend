// Copyright (c) 2012, Joyent, Inc. All rights reserved.
var util = require('util'),
    async = require('async'),
    restify = require('restify'),
    Logger = require('bunyan'),
    wf = require('wf'),
    WorkflowBackend = wf.WorkflowBackend;

var sprintf = util.format;

var WorkflowMorayBackend = module.exports = function (config) {
  WorkflowBackend.call(this);
  if (!config.logger) {
    config.logger = {};
  }

  config.logger.name = 'wf-pg-backend';
  config.logger.serializers = {
    err: Logger.stdSerializers.err
  };

  config.logger.streams = config.logger.streams || [ {
    level: 'info',
    stream: process.stdout
  }];

  this.log = new Logger(config.logger);
  this.config = config;
  this.client = null;
};

util.inherits(WorkflowMorayBackend, WorkflowBackend);

WorkflowMorayBackend.prototype.init = function (callback) {
  var self = this,
      port = self.config.port || 8080,
      host = self.config.host || '127.0.0.1',
      url = 'http://' + host + ':' + port,
      moray = require('moray-client');

  self.client = moray.createClient({
    url: url,
    log: self.log,
    connectTimeout: self.config.connectTimeout
  });

  return self._createBuckets(callback);
};


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
    wf_workflows: function (cbk) {
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
              return cbk(err);
            } else {
              return cbk(null, true);
            }
          });
        } else {
          return cbk(null, false);
        }
      });
    },
    wf_jobs: function (cbk) {
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
                type: 'string'
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
                ['name', 'chain', 'workflow_uuid'].forEach(function (field) {
                  if (!req.value[field]) {
                    return cb(new restify.MissingParameterError(field +
                        ' is required'));
                  }
                });

                if (!req.value.created_at) {
                  req.value.created_at = new Date().toISOString();
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
                                'running'];
                if (!exec || statuses.indexOf(exec) !== -1) {
                  return cb();
                } else {
                  return cb(
                    new restify.InvalidArgumentError('execution is invalid'));
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
                  return cb(
                    new restify.InvalidArgumentError('workflow_uuid is invalid'));
                } 
              },
              function checkValidDates(req, cb) {
                var dates = ['exec_after', 'created_at'];
                dates.forEach(function (d) {
                  var day = new Date(d);
                  if (!util.isDate(day)) {
                    return cb(
                      new restify.InvalidArgumentError(d + ' is invalid'));
                  }
                });
                return cb(null);
              }
            ]
          }, function (err) {
            if (err) {
              return cbk(err);
            } else {
              return cbk(null, true);
            }
          });
        } else {
          return cbk(null, false);
        }
      });
    },
    wf_runners: function (cbk) {
      return self._bucketExists('wf_runners', function (exists) {
        if (!exists) {
          return self.client.putBucket('wf_runners', {
            schema: {}
          }, function (err) {
            if (err) {
              return cbk(err);
            } else {
              return cbk(null, true);
            }
          });
        } else {
          return cbk(null, false);
        }
      });

    }
      };

  async.series(series, function (err, results) {
    if (err) {
      return callback(err);
    } else {
      console.log(util.inspect(results, false, 8));
      return callback(null);
    }
  });
};


// Callback - f(err, res);
WorkflowMorayBackend.prototype.quit = function (callback) {
  callback();
};
