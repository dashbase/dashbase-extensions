package io.dashbase.firehose.kafka_10;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaOffset {
  public Map<Integer, AtomicLong> offsetMap = new HashMap<>();
}
