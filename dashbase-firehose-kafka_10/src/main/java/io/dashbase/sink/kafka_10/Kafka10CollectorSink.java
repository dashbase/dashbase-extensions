package io.dashbase.sink.kafka_10;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.dashbase.firehose.kafka_10.Kafka10Firehose;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import io.dashbase.collector.CollectorSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Kafka10CollectorSink extends CollectorSink {

  private static final Logger logger = LoggerFactory.getLogger(Kafka10Firehose.class);

  KafkaSinkConfig config;
  private KafkaProducer<byte[], byte[]> producer = null;

  @Override
  protected void doAdd(String name, Map<String, String> params, byte[] data, boolean isBatch)
      throws Exception {

  }

  @Override
  public void configure(Map<String, Object> params) {
    logger.info("kafka sink configuration: " + params);
    super.configure(params);
    ObjectMapper mapper = new ObjectMapper();
    config = mapper.convertValue(params, KafkaSinkConfig.class);
    Preconditions.checkNotNull(config);
    setProducer(buildProducer(config));
  }

  @Override
  public void shutdown() throws Exception {
    if (producer != null) {
      producer.close();
    }
  }

  @Override
  public void registerMetrics(MetricRegistry metricRegistry) {
    super.registerMetrics(metricRegistry);
    Map<MetricName, ? extends Metric> metrics = producer.metrics();
    for (final Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
      MetricName metricName = entry.getKey();
      StringBuilder key = new StringBuilder();

      key.append("sink.kafka.")
          .append(metricName.group()).append(".")
          .append(metricName.name());

      metricRegistry.register(key.toString(), (Gauge<Double>) () -> entry.getValue().value());
    }
  }

  @VisibleForTesting
  void setProducer(Producer<byte[], byte[]> producer) {
    this.producer = (KafkaProducer<byte[], byte[]>) producer;
  }

  private static KafkaProducer<byte[], byte[]> buildProducer(KafkaSinkConfig config) {
    Properties props = new Properties();
    props.putAll(config.kafkaProps);
    props.put("bootstrap.servers", config.hosts);
    props.put("acks", config.acks);
    props.put("batch.size", config.batchSize);
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
    KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
    return producer;
  }
}
