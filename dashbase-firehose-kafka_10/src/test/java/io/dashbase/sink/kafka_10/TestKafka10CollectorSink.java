package io.dashbase.sink.kafka_10;

import com.google.common.base.Charsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;
import junit.framework.Assert;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

public class TestKafka10CollectorSink {

  @Test
  public void testBasic() throws Exception {
    MockProducer<byte[], byte[]> mockProducer = new MockProducer<>(true, null, null);
    String acks = "0";
    int batchSize = 16384;
    int numRecords = 10;
    String topic = "test";

    Kafka10CollectorSink sink = new Kafka10CollectorSink() {

      @Override
      public void configure(Map<String, Object> params) {
        this.config = new KafkaSinkConfig();
        this.config.acks = acks;
        this.config.batchSize = batchSize;
        setProducer(mockProducer);
      }

    };

    sink.configure(null);

    sink.start();

    for (int i = 0; i < numRecords; i++) {
      byte[] data = String.valueOf(i).getBytes(Charsets.UTF_8);
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, data);
      Future<RecordMetadata> metadata = mockProducer.send(record);
      Assert.assertTrue("record should send immediately", metadata.isDone());
      //Assert.assertEquals("offset should be equivalent " + i, i, metadata.get().offset());
      //Assert.assertEquals(topic, metadata.get().topic());
      Assert.assertEquals("record is in producer history", record,mockProducer.history().get(i));
    }

    mockProducer.clear();
    Assert.assertEquals("clear erases history", 0, mockProducer.history().size());

    sink.shutdown();
  }
}
