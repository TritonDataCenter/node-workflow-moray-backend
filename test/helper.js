/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var path = require('path'),
    fs = require('fs'),
    existsSync = fs.existsSync || path.existsSync;
var cfg = path.resolve(__dirname, './config.json'),
    cfg_file = existsSync(cfg) ? cfg :
               path.resolve(__dirname, './config.json.sample'),
               config;

module.exports = {
    config: function () {
        if (!config) {
            config = JSON.parse(fs.readFileSync(cfg_file, 'utf-8'));
        }
        return config;
    }
};
