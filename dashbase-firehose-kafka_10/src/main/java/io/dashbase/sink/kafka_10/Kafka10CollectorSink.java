package io.dashbase.sink.kafka_10;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import io.dashbase.collector.CollectorSink;

public class Kafka10CollectorSink extends CollectorSink
{

  private KafkaProducer producer = null;
  
  @Override
  protected void doAdd(String name, Map<String, String> params, byte[] data, boolean isBatch) throws Exception
  {
    
  }

  @Override
  public void configure(Map<String, Object> params)
  {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void shutdown() throws Exception
  {
    if (producer != null) {
      producer.close();
    }
  }

  @Override
  public void registerMetrics(MetricRegistry metricRegistry)
  {
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
}
