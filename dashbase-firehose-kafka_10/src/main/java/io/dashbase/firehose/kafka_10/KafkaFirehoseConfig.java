package io.dashbase.firehose.kafka_10;

import java.util.Map;

import com.google.common.collect.Maps;

public class KafkaFirehoseConfig {
  public String hosts;
  public String groupId;
  public String topic;
  public int pollIntervalMs = Kafka10Firehose.DEFAULT_POLL_INTERVAL_MS;
  public Map<String, String> kafkaProps = Maps.newHashMap();
}
