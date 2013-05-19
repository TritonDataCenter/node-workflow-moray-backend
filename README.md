A backend for [node-workflow](http://kusor.github.com/node-workflow/) built
over [Moray](http://).

# Installation

    npm install wf-moray-backend

      ^
      |__ Not yet :-)

# Usage

Add the following to the config file of your application using wf:

    {
      "backend": {
        "module": "wf-moray-backend",
        "opts": {
          "url": "http://127.0.0.1:8080",
          "connectTimeout": 1000
        }
      }
    }

Where `url` is the url where moray server is listening.

And that should be it. `wf` REST API and Runners should take care of
properly loading the module on init.

# Issues

See [node-workflow issues](https://github.com/kusor/node-workflow/issues).


# Testing

### Setup a `moray_test` db from manatee zone:

    [root@headnode (coal) ~]# sdc-login manatee
    [root@... (coal:manatee0) ~]# createdb -U postgres moray_test
    [root@... (coal:manatee0) ~]# psql -U postgres moray_test
    moray_test=# CREATE TABLE buckets_config (
        name text PRIMARY KEY,
        index text NOT NULL,
        pre text NOT NULL,
        post text NOT NULL,
        options text,
        mtime timestamp without time zone DEFAULT now() NOT NULL
    );


### Boot another moray instance pointing to the `moray_test` db:

    [root@headnode (coal) ~] sdc-login moray
    [root@... (coal:moray0) ~]# cd /opt/smartdc/moray/
    [root@... (coal:moray0) /opt/smartdc/moray]# cp etc/config.json etc/config.test.json
    [root@... (coal:moray0) /opt/smartdc/moray]# cp smf/manifests/moray.xml smf/manifests/moray-test.xml

Then, you need to modify SMF to make this moray test instance to use our `moray_test` db
and listen into a different port:

0. Change service name from `moray` to `moray-test` at the very top of the manifest:

    <?xml version="1.0"?>
    <!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
    <service_bundle type="manifest" name="smartdc-moray-test">
        <service name="smartdc/application/moray-test" type="service" version="1">

1. Make sure the `start` method `exec` attribute has the following value:

    exec="node main.js -f etc/config.json -p 2222 &amp;"

2. Add `MORAY_DB_NAME` to the `start` method environment:

    <envvar name="MORAY_DB_NAME" value="moray_test" />

3. Import the new service:

    svccfg import smf/manifests/moray-test.xml

You should have a new moray instance running at the moray zone on port 2222 now. For
COAL this means your moray URL will be `http://10.99.99.17:2222`.


# LICENSE

The MIT License (MIT) Copyright (c) 2012 Pedro Palazón Candel

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

