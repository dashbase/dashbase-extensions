package io.dashbase.firehose.kafka_10;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

public class OffsetManager
{
  private ObjectMapper objMapper = new ObjectMapper();
  private Map<String, Map<Integer, AtomicLong>> offsetMap = Maps.newHashMap();
  private String currentTopic;
  private int currentPartition;
  private AtomicLong currentOffset = new AtomicLong(0L);
  
  public void updateOffset(ConsumerRecord<?,?> record) {
    if (record != null) {
      updateOffset(record.topic(), record.partition(), record.offset());
    }
  }
  public void updateOffset(String topic, int partition, long offset) {
    
  }
}
