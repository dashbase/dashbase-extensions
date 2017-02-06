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
2. Run firehose_runner
  ```bash
  $DASHBASE_HOME/dashbase/bin/firehose_runner.sh -f $(pwd)/conf/cloudwatch_firehose.json -p $(pwd)/conf/cloudwatch_parser.json
  ```
