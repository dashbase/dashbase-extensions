package io.dashbase.firehose.syslog;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.graylog2.syslog4j.server.SyslogServer;
import org.graylog2.syslog4j.server.SyslogServerConfigIF;
import org.graylog2.syslog4j.server.SyslogServerEventHandlerIF;
import org.graylog2.syslog4j.server.SyslogServerEventIF;
import org.graylog2.syslog4j.server.SyslogServerIF;
import org.graylog2.syslog4j.server.SyslogServerSessionEventHandlerIF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import rapid.firehose.RapidFirehose;

public class SyslogFirehose extends RapidFirehose {

  private static Logger logger = LoggerFactory.getLogger(SyslogFirehose.class);
  private String protocol = "udp";
  private String host = "127.0.0.1";
  private int port = DEFAULT_PORT;
  
  // being very generous at 1M elements in buffer before blocking.
  private static final int DEFAULT_MAX_BUFFER_SIZE = 1024*1024;
  private static final int DEFAULT_PORT= 32376;
  
  // unbounded, will let the server control the size
  private final LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>(DEFAULT_MAX_BUFFER_SIZE);

  private Meter blockMeter = null;
  private Meter bytesMeter = null;
  private Meter requestMeter = null;
  private Meter eventProduceMeter = null;
  
  private SyslogServerIF threadedInstance = null;
  
	public SyslogFirehose() {
	}
	
	@Override
  public String getName() {
    return "syslog";
  }

	@Override
	public void registerMetrics(MetricRegistry metricRegistry) {
	  super.registerMetrics(metricRegistry);
	  metricRegistry.register("firehose.syslog.queue.size", (Gauge<Integer>) () -> queue.size());
    blockMeter = metricRegistry.meter("firehose.syslog.block");
    bytesMeter = metricRegistry.meter("firehose.syslog.bytes.read");
    requestMeter = metricRegistry.meter("firehose.syslog.requests");
    eventProduceMeter = metricRegistry.meter("firehose.syslog.produce");		
	}

	@Override
	public void configure(Map<String, Object> params) {
	  logger.info("start dashbase syslog firehose server ");
	  super.configure(params);
	  if (params != null) {
	    if (params.containsKey("port")) {
        try {
          port = Integer.parseInt(String.valueOf(params.get("port")));
        } catch(Exception e) {
          logger.error("problem parsing port, defaulting to " + DEFAULT_PORT, e);
        }
      }
	    if (params.containsKey("protocol")) {
	      protocol = (String) params.get("protocol");      
	    }
	    if (params.containsKey("host")) {
        host = (String) params.get("host");      
      }
	  }    
	}

    @Override
    public byte[] doNext() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            logger.warn("waiting on queue interrupted.", e);
            return null;
        }
    }

	@Override
	public void start() throws Exception {
	  logger.info("starting syslog firehose");
	  final SyslogServerIF syslogServer = SyslogServer.getInstance(protocol);

    final SyslogServerConfigIF syslogServerConfig = syslogServer.getConfig();
    syslogServerConfig.setHost(host);
    syslogServerConfig.setPort(port);

    SyslogServerEventHandlerIF eventHandler = new SyslogServerEventHandler(queue);
    syslogServerConfig.addEventHandler(eventHandler);

    threadedInstance = SyslogServer.getThreadedInstance(protocol);
    logger.info("syslog firehose started");
	}

	@Override
	public void shutdown() throws Exception {
	  logger.info("shutting down syslog firehose");
	  try {
	    if (threadedInstance != null) {
	      threadedInstance.shutdown();
        threadedInstance.getThread().join();
	    }	    
    } catch (InterruptedException e) {
      logger.warn("Interrupted while joining syslog server thread", e);
      Thread.currentThread().interrupt();
    }
	  logger.info("syslog listener shutdown");
	  logger.info("waiting for items in the queue to be ingested.");
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
    logger.info("syslog firehose shutdown");
	}
	
	class SyslogServerEventHandler implements SyslogServerSessionEventHandlerIF {
	  private final Queue<byte[]> queue;

	  SyslogServerEventHandler(Queue<byte[]> queue) {
	      this.queue = queue;
	  }

	  @Override
	  public Object sessionOpened(SyslogServerIF syslogServer, SocketAddress socketAddress) {
	      return null;
	  }

	  @Override
	  public void event(
	        Object session,
	        SyslogServerIF syslogServer,
	        SocketAddress socketAddress,
	        SyslogServerEventIF event) {
	    
	    requestMeter.mark();
      if (queue.size() >= DEFAULT_MAX_BUFFER_SIZE) {        
        if (blockMeter != null) {
          blockMeter.mark();
        }
        return;
      }
	    
	    byte[] bytes = event.getRaw();
	    bytesMeter.mark(bytes.length);
	    try {
	      queue.add(bytes);
	      eventProduceMeter.mark();
	    } catch (Exception e) {
	      logger.error("Exception while adding event to sink", e);
	    }
	  }

	  @Override
	  public void exception(
	        Object session,
	        SyslogServerIF syslogServer,
	        SocketAddress socketAddress,
	        Exception exception) {
	  }

	  @Override
	  public void sessionClosed(
	        Object session,
	        SyslogServerIF syslogServer,
	        SocketAddress socketAddress,
	        boolean timeout) {
	  }

	  @Override
	  public void initialize(SyslogServerIF syslogServer) {
	  }

	  @Override
	  public void destroy(SyslogServerIF syslogServer) {
	  }

	}

}
