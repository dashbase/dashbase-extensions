package io.dashbase.firehose.kafka_10;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

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
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import rapid.firehose.RapidFirehose;

public class Kafka10Firehose extends RapidFirehose {

  private static final Logger logger = LoggerFactory.getLogger(Kafka10Firehose.class);

  KafkaFirehoseConfig config;
  private volatile boolean stop = false;

  private Consumer<byte[], byte[]> consumer;
  private Iterator<ConsumerRecord<byte[], byte[]>> batchIterator = null;

  static final int DEFAULT_POLL_INTERVAL_MS = 100;

  private KafkaOffset offset = new KafkaOffset();
  private int currentPartition = -1;
  private AtomicLong currentOffset = new AtomicLong(0L);

  private final ObjectMapper mapper = new ObjectMapper();

  public byte[] doNext() throws IOException {
    if (batchIterator == null) {
      ConsumerRecords<byte[], byte[]> batch = null;
      while (!stop) {
        batch = consumer.poll(config.pollIntervalMs);
        if (batch == null || batch.isEmpty()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            return null;
          }
        } else {
          break;
        }
      }
      batchIterator = batch.iterator();
    }
    if (!batchIterator.hasNext()) {
      batchIterator = null;
      return doNext();
    } else {
      ConsumerRecord<byte[], byte[]> record = batchIterator.next();
      int recordPartition = record.partition();
      long recordOffset = record.offset();
      if (currentPartition == recordPartition) {
        currentOffset.set(Math.max(currentOffset.get(), recordOffset));
      } else {
        currentPartition = recordPartition;
        currentOffset = offset.offsetMap.get(recordPartition);
        if (currentOffset == null) {
          currentOffset = new AtomicLong(recordOffset);
          offset.offsetMap.put(recordPartition, currentOffset);
        } else {
          currentOffset.set(Math.max(currentOffset.get(), recordOffset));
        }
      }
      return record.value();
    }
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
    this.consumer.subscribe(ImmutableSet.of(config.topic));
  }

  public void shutdown() throws Exception {
    try {
      stop = true;
      Thread.currentThread().interrupt();
    } finally {
      consumer.close();
    }
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
    for (Entry<Integer, AtomicLong> entry : offset.offsetMap.entrySet()) {
      TopicPartition topicPartition = new TopicPartition(config.topic, entry.getKey());
      consumer.seek(topicPartition, entry.getValue().get());
    }
  }

  public String getOffset() throws IOException {
    consumer.commitAsync();
    return mapper.writeValueAsString(offset);
  }

  @Override
  public void registerMetrics(MetricRegistry metricRegistry) {
    super.registerMetrics(metricRegistry);
    Map<MetricName, ? extends Metric> metrics = this.consumer.metrics();
    for (final Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
      MetricName metricName = entry.getKey();
      StringBuilder key = new StringBuilder();

      key.append("firehose.kafka.")
        .append(metricName.group()).append(".")
        .append(metricName.name());

      metricRegistry.register(key.toString(), (Gauge<Double>) () -> entry.getValue().value());
    }
  }

}
