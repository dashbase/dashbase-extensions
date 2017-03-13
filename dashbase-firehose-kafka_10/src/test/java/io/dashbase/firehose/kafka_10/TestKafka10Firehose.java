package io.dashbase.firehose.kafka_10;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import junit.framework.Assert;

public class TestKafka10Firehose {

  private int numRecords = 10;
  private String topic = "test";
  private int partition = 0;

  private MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(
      OffsetResetStrategy.EARLIEST);
  private Kafka10Firehose firehose = new Kafka10Firehose() {

    @Override
    public void configure(Map<String, Object> params) {
      this.config = new KafkaFirehoseConfig();
      this.config.pollIntervalMs = 100;
      this.config.topic = topic;
      setConsumer(mockConsumer);
    }

    @Override
    public void start() {
      mockConsumer.assign(ImmutableSet.of(new TopicPartition(config.topic, partition)));
    }

  };

  @Test
  public void testBasic() throws Exception {

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
      if (c == numRecords) {
        break;
      }
    }
    firehose.shutdown();
  }

  @Test
  public void testKafkaOffset() throws Exception {

    firehose.configure(null);

    firehose.start();

    byte[] data = String.valueOf(0).getBytes(Charsets.UTF_8);
    mockConsumer.addRecord(new ConsumerRecord<byte[], byte[]>(
        topic,
        partition,
        0,
        -1L,
        TimestampType.NO_TIMESTAMP_TYPE,
        0L,
        0,
        data.length,
        null,
        data));

    mockConsumer.updateBeginningOffsets(ImmutableMap.of(new TopicPartition(topic, partition), 0L));

    firehose.next();

    Assert.assertEquals("{\"offsetMap\":{\"0\":0}}", firehose.getOffset());

    data = String.valueOf(1).getBytes(Charsets.UTF_8);
    mockConsumer.addRecord(new ConsumerRecord<byte[], byte[]>(
        topic,
        partition,
        1,
        -1L,
        TimestampType.NO_TIMESTAMP_TYPE,
        0L,
        0,
        data.length,
        null,
        data));

    firehose.next();

    Assert.assertEquals("{\"offsetMap\":{\"0\":1}}", firehose.getOffset());

    firehose.shutdown();
  }

  @Test
  public void testKafkaOffsetMapping() throws Exception {
    KafkaOffset offset = new KafkaOffset();
    offset.offsetMap = new Int2LongOpenHashMap();
    ObjectMapper mapper = new ObjectMapper();
    String str = mapper.writeValueAsString(offset);
    offset = mapper.readValue(str, KafkaOffset.class);

    for (Entry<Integer, Long> entry : offset.offsetMap.entrySet()) {
      Assert.assertEquals(1, entry.getKey().intValue());
      Assert.assertEquals(2L, entry.getValue().longValue());
    }
  }


}
