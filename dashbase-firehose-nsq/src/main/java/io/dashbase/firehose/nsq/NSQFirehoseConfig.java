package io.dashbase.firehose.nsq;

public class NSQFirehoseConfig {
    public String lookupAddress = "localhost";
    public int lookupPort = 4161;

    public String topic;
    public String channel;

    public int queueSize = 1000000;
}
