Dashbase CloudWatch Firehose
======
Stream logs from AWS Cloudwatch to Dashbase

Usage
-----
1. Build
  ```bash
  # maven build
  mvn clean package

  # install
  mvn install
  
  # or deploy
  mvn deploy
  ```

  You are now able to reference it via the *type* param, see [Example conf file](https://github.com/dashbase/dashbase-extensions/blob/master/dashbase-firehose-cloudwatch/conf/cloudwatch_firehose.yml)

2. Configure the group and stream params in `cloudwatch_firehose.yml` to match the desired AWS CloudWatch group and stream name.

3. You can now test it via the firehose_runner.sh tool:

  ~~~~
  $DASHBASE_HOME/bin/firehose_runner.sh -f $(pwd)/conf/cloudwatch_firehose.yml
  ~~~~
