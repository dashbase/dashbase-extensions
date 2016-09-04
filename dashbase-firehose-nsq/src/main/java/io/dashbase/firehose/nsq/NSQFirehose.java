package io.dashbase.firehose.nsq;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rapid.firehose.RapidFirehose;
import rapid.firehose.RapidFirehoseMessage;
import rapid.server.config.Configurable;
import rapid.server.config.Measurable;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brainlag.nsq.NSQConsumer;
import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.lookup.DefaultNSQLookup;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.google.common.base.Preconditions;

public class NSQFirehose extends RapidFirehose
      implements Configurable, Measurable, Iterator<RapidFirehoseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(NSQFirehose.class);

    private final NSQLookup nsqLookup;

    private NSQConsumer nsqConsumer;
    private NSQFirehoseConfig nsqConfig;

    private BlockingQueue<NSQMessage> blockingQueue = new LinkedBlockingQueue<>();
    
    private Meter eventConsumpMeter = null;
    private Meter eventProduceMeter = null;
    

    public NSQFirehose() {
        this.nsqLookup = new DefaultNSQLookup();
    }

    @Override
    public Iterator<RapidFirehoseMessage> iterator() {
        return this;
    }

    @Override
    public void start() throws Exception {        
        nsqLookup.addLookupAddress(nsqConfig.lookupAddress, nsqConfig.lookupPort);
        nsqConsumer = new NSQConsumer(
              nsqLookup,
              nsqConfig.topic,
              nsqConfig.channel,
              (message) -> {
                  try {
                	  if (logger.isDebugEnabled()) {
                        logger.debug("consumer msg: {}", new String(message.getMessage()));
                	  }
                      blockingQueue.put(message);
                      eventProduceMeter.mark();
                  } catch (InterruptedException e) {
                      logger.error("Interrupted while enqueueing message", e);
                      Thread.currentThread().interrupt();
                  }
              });
        nsqConsumer.start();

        logger.info("Started NSQ consumer");
    }

    @Override
    public void shutdown() throws Exception {
    	logger.info("draining internal data queue");
        int queueSize;
        int countDown = 5;
        while ((queueSize = blockingQueue.size()) > 0 && countDown > 0) {
          try {
            if (queueSize > 0) {
              logger.info("queue is still not empty: " + queueSize + ", waiting 5s, count down: " + countDown);
              Thread.sleep(5000);
            } else {
              break;
            }
          } catch(Exception e) {
            logger.error("drain thread interrupted, queue size: " + queueSize);
            break;
          } finally {
            countDown--;
          }
        }
        
        if ((queueSize = blockingQueue.size()) > 0) {
          logger.error("queue is not empty, size = "  + queueSize + ", possible data loss");
        }
        nsqConsumer.shutdown();
        logger.info("Shutdown NSQ consumer");
    }

    @Override
    public void configure(Map<String, Object> params) {
        logger.info("NSQ firehose configuration: " + params);
        ObjectMapper mapper = new ObjectMapper();
        nsqConfig = mapper.convertValue(params, NSQFirehoseConfig.class);
        Preconditions.checkNotNull(nsqConfig);
        blockingQueue = new LinkedBlockingQueue<>(nsqConfig.queueSize);
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public RapidFirehoseMessage next() {
        try {
            final NSQMessage message = blockingQueue.take();
            message.finished();
            eventConsumpMeter.mark();
            return new RapidFirehoseMessage() {
                @Override
                public String offset() {
                    // Offset is not supported.
                    return null;
                }

                @Override
                public byte[] data() {
                    return message.getMessage();
                }
            };
        } catch (InterruptedException e) {
            logger.error("Interrupted while taking message", e);
            Thread.currentThread().interrupt();
        }

        return null;
    }

    @Override
    public void seekToOffset(String offset) {
    	logger.warn("nsq firehose does not seek");
    }

	@Override
	public void registerMetrics(MetricRegistry metricRegistry) {
		metricRegistry.register("firehose.nsq.queue.size", (Gauge<Integer>) () -> blockingQueue.size());
		eventConsumpMeter = metricRegistry.meter("firehose.nsq.consume");
	    eventProduceMeter = metricRegistry.meter("firehose.jsq.produce");
	}
}
