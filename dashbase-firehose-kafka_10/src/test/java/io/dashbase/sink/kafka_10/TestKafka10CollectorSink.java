package io.dashbase.sink.kafka_10;

import java.util.Map;
import org.apache.kafka.clients.producer.MockProducer;
import org.junit.Test;

public class TestKafka10CollectorSink {

  @Test
  public void testBasic() throws Exception {
    MockProducer<byte[], byte[]> mockProducer = new MockProducer<>(true, null, null);
    String acks = "0";
    int batchSize = 16384;

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

    sink.shutdown();
  }
}
