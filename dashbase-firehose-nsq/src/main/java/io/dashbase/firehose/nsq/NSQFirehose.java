package io.dashbase.firehose.nsq;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rapid.firehose.RapidFirehose;
import rapid.firehose.RapidFirehoseMessage;
import rapid.server.config.Configurable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brainlag.nsq.NSQConsumer;
import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.lookup.DefaultNSQLookup;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.google.common.base.Preconditions;

public class NSQFirehose extends RapidFirehose
      implements Configurable, Iterator<RapidFirehoseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(NSQFirehose.class);

    private final NSQLookup nsqLookup;

    private NSQConsumer nsqConsumer;
    private NSQFirehoseConfig nsqConfig;

    private BlockingQueue<NSQMessage> blockingQueue;

    public NSQFirehose() {
        this.nsqLookup = new DefaultNSQLookup();
    }

    @Override
    public Iterator<RapidFirehoseMessage> iterator() {
        return this;
    }

    @Override
    public void start() throws Exception {
        blockingQueue = new ArrayBlockingQueue<>(nsqConfig.queueSize);

        nsqLookup.addLookupAddress(nsqConfig.lookupAddress, nsqConfig.lookupPort);
        nsqConsumer = new NSQConsumer(
              nsqLookup,
              nsqConfig.topic,
              nsqConfig.channel,
              (message) -> {
                  try {
                      logger.info("consumer msg: {}", new String(message.getMessage()));
                      blockingQueue.put(message);
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
        nsqConsumer.shutdown();
        logger.info("Shutdown NSQ consumer");
    }

    @Override
    public void configure(Map<String, Object> params) {
        logger.info("NSQ firehose configuration: " + params);
        ObjectMapper mapper = new ObjectMapper();
        nsqConfig = mapper.convertValue(params, NSQFirehoseConfig.class);
        Preconditions.checkNotNull(nsqConfig);
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
        throw new UnsupportedOperationException(
              "seekToOffset is not supported by " + getClass().getSimpleName());
    }
}
