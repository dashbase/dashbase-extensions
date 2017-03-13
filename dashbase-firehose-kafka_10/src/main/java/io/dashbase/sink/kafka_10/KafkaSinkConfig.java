package io.dashbase.sink.kafka_10;

import com.google.common.collect.Maps;
import java.util.Map;

public class KafkaSinkConfig {
  public String hosts;
  public String acks;
  public int batchSize;
  public String topic;
  public Map<String, String> kafkaProps = Maps.newHashMap();
}
