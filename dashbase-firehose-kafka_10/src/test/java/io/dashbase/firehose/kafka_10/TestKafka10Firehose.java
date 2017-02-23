package io.dashbase.firehose.kafka_10;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

import junit.framework.Assert;

public class TestKafka10Firehose
{
  @Test
  public void testBasic() throws Exception {
    MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);    
    int numRecords = 10;
    String topic = "test";
    int partition = 0;
    
    Kafka10Firehose firehose = new Kafka10Firehose() {

      @Override
      public void configure(Map<String, Object> params)
      {
        this.config = new KafkaFirehoseConfig();
        this.config.pollIntervalMs = 100;
        this.config.topic = topic;
        setConsumer(mockConsumer);
      }
      
    };
    
    firehose.configure(null);
    
    firehose.start();    
    
    for (int i = 0; i < numRecords; ++i) {
      byte[] data = String.valueOf(i).getBytes(Charsets.UTF_8);
      mockConsumer.addRecord(new ConsumerRecord<byte[], byte[]>(
          topic, 
          partition, 
          i, 
          -1L, 
          TimestampType.NO_TIMESTAMP_TYPE, 
          0L, 
          0, 
          data.length, 
          null, 
          data));
    }
    
    mockConsumer.updateBeginningOffsets(ImmutableMap.of(new TopicPartition(topic, partition), 0L));
    byte[] readData = null;
    int c = 0;
    while ((readData = firehose.next()) != null) {
      int readVal = Integer.parseInt(new String(readData, Charsets.UTF_8));
      Assert.assertEquals(c, readVal);
      c++;
      if (c == numRecords) break;
    }    
    firehose.shutdown();    
  }
}
