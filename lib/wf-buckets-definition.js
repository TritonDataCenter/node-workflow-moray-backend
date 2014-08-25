/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

// Attempt to:
// - Automate encode/decode objects to/from moray
// - Allow definition of attributes as "object" (for jstypes Oject and Array)
// - Move buckets definition code to a separate place for clarity
// - Allow definition of arbitrary fields for each setup, which don't
//   necessarily need to be indexed. Indeed, it's desirable to do not index
//   objects.

var util = require('util');

function genericDecode(obj, fields) {
    var p;
    for (p in obj) {
        if (typeof (fields[p]) !== 'undefined' &&
                fields[p].type === 'object' &&
                typeof (obj[p]) === 'string') {
            try {
                obj[p] = JSON.parse(obj[p]);
            } catch (e) {}
        }
    }
    return (obj);
}

function genericEncode(obj) {
    var p;
    for (p in obj) {
        if (typeof (obj[p]) === 'object') {
            obj[p] = JSON.stringify(obj[p]);
        }
    }
    return (obj);
}

var definitions = {
    wf_workflows: {
        fields: {
            name: {
                type: 'string',
                unique: true
            },
            chain: {
                type: 'object',
                index: false
            },
            onerror: {
                type: 'object',
                index: false
            },
            oncancel: {
                type: 'object',
                index: false
            }
        },
        decode: function (obj, fields) {
            var p;
            for (p in obj) {
                if (typeof (fields[p]) !== 'undefined' &&
                        fields[p].type === 'object' &&
                        typeof (obj[p]) === 'string') {
                    try {
                        obj[p] = JSON.parse(obj[p]);
                    } catch (e) {}
                } else if (p === 'timeout') {
                    obj[p] = Number(obj[p]);
                }
            }
            return (obj);
        },
        encode: function (obj) {
            var p;
            for (p in obj) {
                if (typeof (obj[p]) === 'object') {
                    obj[p] = JSON.stringify(obj[p]);
                }
            }
            return (obj);
        },
        pre: [],
        post: []
    },
    wf_jobs: {
        fields: {
            execution: {
                type: 'string',
                index: true
            },
            workflow_uuid: {
                type: 'string',
                index: true
            },
            created_at: {
                type: 'number',
                index: true
            },
            exec_after: {
                type: 'number',
                index: true
            },
            runner_id: {
                type: 'string',
                index: true
            },
            target: {
                type: 'string',
                index: true
            },
            chain: {
                type: 'object',
                index: false
            },
            onerror: {
                type: 'object',
                index: false
            },
            oncancel: {
                type: 'object',
                index: false
            },
            params: {
                type: 'object',
                index: false
            },
            chain_results: {
                type: 'object',
                index: false
            },
            onerror_results: {
                type: 'object',
                index: false
            },
            oncancel_results: {
                type: 'object',
                index: false
            },
            name: {
                type: 'string',
                index: true
            }
        },
        decode: function (obj, fields) {
            var p;
            for (p in obj) {
                if (typeof (fields[p]) !== 'undefined' &&
                        fields[p].type === 'object' &&
                        typeof (obj[p]) === 'string') {
                    try {
                        obj[p] = JSON.parse(obj[p]);
                    } catch (e) {}
                } else if ((p === 'created_at' || p === 'exec_after') &&
                        typeof (obj[p]) === 'number') {
                    obj[p] = new Date(obj[p]).toISOString();
                }
            }
            return (obj);
        },
        encode: function (obj) {
            var p;
            for (p in obj) {
                if (typeof (obj[p]) === 'object') {
                    obj[p] = JSON.stringify(obj[p]);
                } else if ((p === 'created_at' || p === 'exec_after') &&
                        typeof (obj[p]) === 'string') {
                    obj[p] = new Date(obj[p]).getTime();
                }
            }
            return (obj);
        },
        pre: [],
        post: []
    },
    wf_runners: {
        fields: {
            idle: {
                type: 'boolean',
                index: true
            },
            active_at: {
                type: 'string',
                index: true
            }
        },
        decode: genericDecode,
        encode: genericEncode,
        pre: [],
        post: []
    },
    wf_jobs_info: {
        fields: {
            job_uuid: {
                type: 'string',
                index: true
            },
            created_at: {
                type: 'number',
                index: true
            },
            info: {
                type: 'object',
                index: false
            }
        },
        decode: genericDecode,
        encode: genericEncode,
        pre: [],
        post: []
    },
    wf_locked_targets: {
        fields: {
            target: {
                type: 'string',
                index: true
            }
        },
        decode: genericDecode,
        encode: genericEncode,
        pre: [],
        post: []
    }
};

function _2morayBucket(definition) {
    var moray_bucket_definition = {
        pre: definition.pre || [],
        post: definition.post || [],
        index: {}
    };

    Object.keys(definition.fields).forEach(function (k) {
        if (definition.fields[k].index || definition.fields[k].unique) {
            moray_bucket_definition.index[k] = {
                type: (definition.fields[k].type === 'object') ? 'string' :
                    definition.fields[k].type
            };
            if (definition.fields[k].unique) {
                moray_bucket_definition.index[k].unique = true;
            }
        }
    });

    return (moray_bucket_definition);
}


function setup(definitions, extra_fields) {
    Object.keys(extra_fields).forEach(function (bucket) {
        if (typeof (definitions[bucket]) !== 'undefined') {
            Object.keys(extra_fields[bucket]).forEach(function (field) {
                definitions[bucket].fields[field] = extra_fields[bucket][field];
            });
        }
    });
    return (definitions);
}

module.exports = {
    definitions: definitions,
    _2morayBucket: _2morayBucket,
    setup: setup
};
