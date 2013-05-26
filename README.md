A backend for [node-workflow](http://kusor.github.com/node-workflow/) built
over [Moray](https://mo.joyent.com/docs/moray/master/).

## Highlights regarding other wf-backends:

- Searching `jobs` by `vm_uuid` is faster, given we are using indexed
`job.target` attribute, which always matches `'(target=*' + vm_uuid + '*)'`
for machines' jobs.
- Searching `jobs` on a given time period is also supported using the arguments
`since`, for jobs created _since_ the given time and `until` for jobs created
_until_ the specified time. Note both of these attributes values will be
included into the search, which is made using LDAP filters. Attributes can
take either `epoch` time values or any valid JavaScript Date string. Note that
if the given value cannot be transformed into a valid JavaScript Date, the
argument will be silently discarded.

## Installation

Add `git@git.joyent.com:node-workflow-moray-backend.git` to your wf project
dependencies.

## Usage

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

## Issues

See [node-workflow issues](https://github.com/kusor/node-workflow/issues) for
Open Source project issues, and [WORKFLOW](https://devhub.joyent.com/jira/browse/WORKFLOW)
for Joyent specific issues.


## Testing

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

    exec="node main.js -f etc/config.json -p %{moray-test/port} &amp;"

2. Add `MORAY_DB_NAME` to the `start` method environment:

    <envvar name="MORAY_DB_NAME" value="moray_test" />

3. Change the value for the `port` variable:

         <instance name="default" enabled="true">
          <property_group name="moray-test" type="application">
            <propval name="port" type="astring" value="2222" />
           </property_group>
         </instance>

The manifest diff versus the original will look like:

    [root@... (coal:moray0) /opt/smartdc/moray]# diff -u smf/manifests/moray.xml smf/manifests/moray-test.xml
    --- smf/manifests/moray.xml     2013-05-26 08:17:06.323940448 +0000
    +++ smf/manifests/moray-test.xml        2013-05-26 09:04:17.859967502 +0000
    @@ -1,7 +1,7 @@
     <?xml version="1.0"?>
     <!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">
    -<service_bundle type="manifest" name="smartdc-moray">
    -    <service name="smartdc/application/moray" type="service" version="1">
    +<service_bundle type="manifest" name="smartdc-moray-test">
    +    <service name="smartdc/application/moray-test" type="service" version="1">

             <dependency name="network"
                         grouping="require_all"
    @@ -27,7 +27,7 @@
             <exec_method
                 type="method"
                 name="start"
    -            exec="node main.js -f etc/config.json -p %{moray/port} &amp;"
    +            exec="node main.js -f etc/config.json -p %{moray-test/port} &amp;"
                 timeout_seconds="30">
                 <method_context working_directory="/opt/smartdc/moray">
                     <method_credential user="nobody"
    @@ -38,6 +38,7 @@
                                 value="/opt/smartdc/moray/build/node/bin:/opt/local/bin:/usr/bin:/usr/sbin:/bin"/>
                         <envvar name="LD_PRELOAD_32"
                                 value="/usr/lib/extendedFILE.so.1" />
    +                    <envvar name="MORAY_DB_NAME" value="moray_test" />
                     </method_environment>
                 </method_context>
             </exec_method>
    @@ -51,8 +52,8 @@
             <property_group name="application" type="application" />

             <instance name="default" enabled="true">
    -          <property_group name="moray" type="application">
    -            <propval name="port" type="astring" value="2020" />
    +          <property_group name="moray-test" type="application">
    +            <propval name="port" type="astring" value="2222" />
               </property_group>
             </instance>


4. Finally, import the new service:

    svccfg import smf/manifests/moray-test.xml

You should have a new moray instance running at the moray zone on port 2222 now. For
COAL this means your moray URL will be `http://10.99.99.17:2222`.


## LICENSE

The MIT License (MIT) Copyright (c) 2012 Pedro Palazón Candel

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

