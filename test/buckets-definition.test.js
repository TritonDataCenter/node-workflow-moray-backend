// Copyright (c) 2013, Joyent, Inc. All rights reserved.
var test = require('tap').test;
var util = require('util');
var buckets = require('../lib/wf-buckets-definition');

var helper = require('./helper');
var config = helper.config();
var definitions = buckets.definitions;

test('setup', function (t) {
    t.ok(config.backend.opts.extra_fields);
    var extra_fields = config.backend.opts.extra_fields;
    t.ok(definitions);
    definitions = buckets.setup(definitions, extra_fields);
    Object.keys(extra_fields).forEach(function (bucket) {
        Object.keys(extra_fields[bucket]).forEach(function (field) {
            t.ok(definitions[bucket].fields[field]);
        });
    });
    t.end();
});

test('to moray bucket', function (t) {
    var jobs_bucket = buckets._2morayBucket(definitions.wf_jobs);
    t.ok(jobs_bucket);
    t.ok(jobs_bucket.pre);
    t.ok(jobs_bucket.index);
    var index = jobs_bucket.index;
    t.ok(index.workflow_uuid);
    t.ok(index.vm_uuid);
    t.ok(index.server_uuid);
    t.equal(typeof (index.chain_results), 'undefined');
    t.equal(typeof (index.onerror_results), 'undefined');
    t.end();
});
