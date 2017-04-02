package io.dashbase.firehose.kafka_10;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import rapid.firehose.RapidFirehose;

public class Kafka10Firehose extends RapidFirehose {

  private static final Logger logger = LoggerFactory.getLogger(Kafka10Firehose.class);

  KafkaFirehoseConfig config;

  private Consumer<byte[], byte[]> consumer;
  private Iterator<ConsumerRecord<byte[], byte[]>> batchIterator = null;

  static final int DEFAULT_POLL_INTERVAL_MS = 100;

  private KafkaOffset offset = new KafkaOffset();
  
  

  private final ObjectMapper mapper = new ObjectMapper();

  public byte[] doNext() throws IOException {
    if (batchIterator == null || !batchIterator.hasNext()) {
      ConsumerRecords<byte[], byte[]> batch;
      do {
        if (Thread.currentThread().isInterrupted()) {
          return null;
        }
        batch = consumer.poll(config.pollIntervalMs);
      } while (batch == null || batch.isEmpty());
      batchIterator = batch.iterator();
    }

    ConsumerRecord<byte[], byte[]> record = batchIterator.next();
    offset.offsetMap.put(record.partition(), record.offset());
    return record.value();
  }

  private static KafkaConsumer<byte[], byte[]> buildConsumer(KafkaFirehoseConfig config) {
    Properties props = new Properties();
    props.putAll(config.kafkaProps);
    props.put("bootstrap.servers", config.hosts);
    props.put("group.id", config.groupId);
    props.put("key.deserializer", ByteArrayDeserializer.class.getName());
    props.put("value.deserializer", ByteArrayDeserializer.class.getName());
    props.put("enable.auto.commit", "false");
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
    return consumer;
  }

  public void start() throws Exception {
    if (this.config.partitions == null || this.config.partitions.isEmpty()) {      
      this.consumer.subscribe(ImmutableSet.of(config.topic));
    } else {
      this.consumer.assign(config.partitions.stream().map(
          e -> new TopicPartition(config.topic, e.intValue())).collect(Collectors.toList()));
    }
  }

  public void shutdown() throws Exception {
    consumer.close();
  }

  @VisibleForTesting
  void setConsumer(Consumer<byte[], byte[]> consumer) {
    this.consumer = consumer;
  }

  public void configure(Map<String, Object> params) {
    logger.info("kafka firehose configuration: " + params);
    super.configure(params);
    ObjectMapper mapper = new ObjectMapper();
    config = mapper.convertValue(params, KafkaFirehoseConfig.class);
    Preconditions.checkNotNull(config);
    setConsumer(buildConsumer(config));
  }

  public void seekToOffset(String offsetString) throws IOException {
    offset = mapper.readValue(offsetString, KafkaOffset.class);
    for (Entry<Integer, Long> entry : offset.offsetMap.entrySet()) {
      TopicPartition topicPartition = new TopicPartition(config.topic, entry.getKey());
      consumer.seek(topicPartition, entry.getValue() + 1);
    }
  }

  public String getOffset() throws IOException {
    return mapper.writeValueAsString(offset);
  }

  @Override
  public void registerMetrics(MetricRegistry metricRegistry) {
    super.registerMetrics(metricRegistry);
    Map<MetricName, ? extends Metric> metrics = this.consumer.metrics();
    for (final Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
      MetricName metricName = entry.getKey();
      String key = "firehose.kafka." + metricName.group() + "." + metricName.name();
      metricRegistry.register(key, (Gauge<Double>) () -> entry.getValue().value());
    }
  }
  
  @Override
  public String getName() {
    return "kafka_10";
  }
}
