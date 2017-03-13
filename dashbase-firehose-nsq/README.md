NSQ firehose plugin for Dashbase.

NSQ if a realtime distributed messaging platform, See details here: [http://nsq.io/](http://nsq.io/)

Installation instructions:

1. mvn clean package
2. mvn install

You are now able to reference it via the *type* param, see [Example conf file](https://github.com/dashbase/dashbase-extensions/blob/master/dashbase-firehose-nsq/conf/nsq_firehose.yml)

You can now test it via the firehose_runner.sh tool:

~~~~
$DASHBASE_HOME/dashbase/bin/firehose_runner.sh $(pwd)/conf/nsq_firehose.yml
~~~~


You should see the output:

~~~~
-- Meters ----------------------------------------------------------------------
firehose.meter
             count = 4
         mean rate = 452.72 events/second
     1-minute rate = 0.00 events/second
     5-minute rate = 0.00 events/second
    15-minute rate = 0.00 events/second
parse.error.meter
             count = 0
         mean rate = 0.00 events/second
     1-minute rate = 0.00 events/second
     5-minute rate = 0.00 events/second
    15-minute rate = 0.00 events/second
parse.exception.meter
             count = 0
         mean rate = 0.00 events/second
     1-minute rate = 0.00 events/second
     5-minute rate = 0.00 events/second
    15-minute rate = 0.00 events/second
~~~~
