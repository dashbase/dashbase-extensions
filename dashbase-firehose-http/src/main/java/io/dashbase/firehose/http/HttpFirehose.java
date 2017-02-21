package io.dashbase.firehose.http;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;
import static spark.Spark.stop;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rapid.components.AbstractServiceComponent;
import rapid.firehose.RapidFirehose;
import rapid.server.config.Configurable;
import rapid.server.config.Measurable;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


public class HttpFirehose extends RapidFirehose
{
  private static Logger logger = LoggerFactory.getLogger(HttpFirehose.class);
  private static final int DEFAULT_PORT = 9999;
  
  // being very generous at 1M elements in buffer before blocking.
  private static final int DEFAULT_MAX_BUFFER_SIZE = 1024*1024;   
  
  private int port = DEFAULT_PORT;
  
  // unbounded, will let the server control the size
  private final LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>(DEFAULT_MAX_BUFFER_SIZE);
  private byte[] nextData;

  private Meter blockMeter = null;
  private Meter bytesMeter = null;
  private Meter requestMeter = null;
  private Meter eventProduceMeter = null;
  
  private AtomicBoolean drained = new AtomicBoolean(false);

  @Override
  public void seekToOffset(String offset)
  {
    logger.warn("http firehose does not seek");
  }

  @Override
  public byte[] doNext()
  {
    try {
      return queue.take();
    } catch (InterruptedException e) {
      logger.warn("waiting on queue interrupted.", e);
      return null;
    }
  }

  @Override
  public void start() throws Exception
  {
    logger.info("starting http firehose");
    runServer(port);
    logger.info("http firehose started");
  }

  @Override
  public void shutdown() throws Exception
  {
    logger.info("stopping http firehose ");
    
    logger.info("shutting down http server to stop receiving data");
    stop();
    
    logger.info("draining internal data queue");
    int queueSize;
    int countDown = 5;
    while ((queueSize = queue.size()) > 0 && countDown > 0) {
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
    
    if ((queueSize = queue.size()) > 0) {
      logger.error("queue is not empty, size = "  + queueSize + ", possible data loss");
    }
    logger.info("http firehose shutdown");
  }

  @Override
  public void configure(Map<String, Object> params) {
      logger.info("start dashbase http firehose server ");
      if (params != null && params.containsKey("port")) {
      	try {
      		port = Integer.parseInt(String.valueOf(params.get("port")));
      	} catch(Exception e) {
      	  logger.error("problem parsing port, defaulting to " + DEFAULT_PORT, e);
      	}
      }
  }
  
  protected byte[][] decompose(byte[] bytes) {
    return new byte[][]{bytes};
  }
  
  protected byte[] transform(byte[] bytes) {
    return bytes;
  }

  private void runServer(int port)
  {
    port(port);
    post("/insert", (request, response) -> {
      requestMeter.mark();
      if (queue.size() >= DEFAULT_MAX_BUFFER_SIZE) {
        response.status(403);
        if (blockMeter != null) {
          blockMeter.mark();
        }
        return "block";
      }
      
      byte[] dataBlock = request.bodyAsBytes();      
      if (dataBlock != null && dataBlock.length > 0) {
        bytesMeter.mark(dataBlock.length);
        byte[][] dataList = decompose(dataBlock);
      
        for (byte[] dataElement : dataList) {          
          queue.add(transform(dataElement));
          eventProduceMeter.mark();
        }
      }
      return "ok";
    });
    get("/ping", (request, response) -> "pong");
  }

  @Override
  public void registerMetrics(MetricRegistry metricRegistry)
  {
    super.registerMetrics(metricRegistry);
    metricRegistry.register("firehose.http.queue.size", (Gauge<Integer>) () -> queue.size());
    blockMeter = metricRegistry.meter("firehose.http.block");
    bytesMeter = metricRegistry.meter("firehose.http.bytes.read");
    requestMeter = metricRegistry.meter("firehose.http.requests");
    eventProduceMeter = metricRegistry.meter("firehose.http.produce");
  }
}
