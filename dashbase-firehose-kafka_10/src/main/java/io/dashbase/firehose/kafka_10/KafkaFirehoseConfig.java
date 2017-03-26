package io.dashbase.firehose.kafka_10;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

public class KafkaFirehoseConfig {
  public String hosts;
  public String groupId;
  public String topic;
  public int pollIntervalMs = Kafka10Firehose.DEFAULT_POLL_INTERVAL_MS;
  public Set<Integer> partitions = Collections.emptySet();
  public Map<String, String> kafkaProps = Maps.newHashMap();
}
