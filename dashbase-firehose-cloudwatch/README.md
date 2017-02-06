Dashbase CloudWatch Firehose
======
Stream logs from AWS Cloudwatch to Dashbase

Usage
-----
1. Build and copy jar to lib.
  ```bash
  # maven build
  mvn clean package
  
  # copy to dashbase lib where $DASHBASE_HOME is the path to the dashbase-engine directory
  cp target/dashbase-firehose-cloudwatch-0.0.1-SNAPSHOT.jar $DASHBASE_HOME/dashbase/target/lib
  ```
  
  You are now able to reference it via the *clazz* param, see [Example conf file](https://github.com/dashbase/dashbase-extensions/blob/master/dashbase-firehose-cloudwatch/conf/cloudwatch_firehose.json)
  
2. Configure the group and stream params in `cloudwatch_firehose.json` to match the desired AWS CloudWatch group and stream name.
  
3. You can now test it via the firehose_runner.sh tool:
  
  ~~~~
  $DASHBASE_HOME/dashbase/bin/firehose_runner.sh -f $(pwd)/conf/cloudwatch_firehose.json -p $(pwd)/conf/cloudwatch_parser.json
  ~~~~
