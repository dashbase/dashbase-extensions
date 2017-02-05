package io.dashbase.firehose.cloudwatch;

import java.util.Iterator;
import java.util.Map;

import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rapid.components.AbstractServiceComponent;
import rapid.firehose.RapidFirehose;
import rapid.server.config.Configurable;

import com.amazonaws.services.logs.*;
import rapid.server.config.Measurable;


public class CloudWatchFirehose implements RapidFirehose, Configurable, Measurable, AbstractServiceComponent {
  private static Logger logger = LoggerFactory.getLogger(CloudWatchFirehose.class);
  private static ObjectMapper mapper = new ObjectMapper();

  private String logGroupName;
  private String logStreamName;
  private AWSLogs cloudWatchClient;

  private GetLogEventsResult result;
  private Iterator<OutputLogEvent> eventsIterator;



  @Override
  public void seekToOffset(String offset) {
    logger.warn("cloudwatch firehose does not seek");
  }

  @Override
  public byte[] next() throws JsonProcessingException {
    // MAX response of 1 MB, or 10,000 events
    if (eventsIterator.hasNext()) {
      final String jsonMessage = mapper.writeValueAsString(eventsIterator.next());
      return jsonMessage.getBytes();
    } else {
      String fwdToken;
      if ((fwdToken = result.getNextForwardToken()) != null) {
        // get the next batch of 10,000 events
        result = result.withNextForwardToken(fwdToken);
        eventsIterator = result.getEvents().iterator();
        if (eventsIterator.hasNext()) {
          final String jsonMessage = mapper.writeValueAsString(eventsIterator.next());
          return jsonMessage.getBytes();
        }
      }
      logger.info("finished ingesting all events from cloudwatch");
      return null;
    }
  }

  @Override
  public void configure(Map<String, Object> params) {
    logger.info("setting cloudwatch firehose configurations");
    logGroupName = Preconditions.checkNotNull((String) params.get("log_group_name"));
    logStreamName = Preconditions.checkNotNull((String) params.get("log_stream_name"));
  }

  @Override
  public void registerMetrics(MetricRegistry metricRegistry) {
    
  }

  @Override
  public void start() throws Exception {
    logger.info("starting cloudwatch client");
    cloudWatchClient = AWSLogsClientBuilder.defaultClient();
    result = cloudWatchClient.getLogEvents(new GetLogEventsRequest()
      .withLogGroupName(logGroupName)
      .withLogStreamName(logStreamName));

    eventsIterator = result.getEvents().iterator();
    logger.info("cloudwatch client started");

  }

  @Override
  public void shutdown() throws Exception {
    logger.info("stopping cloudwatch client");
    cloudWatchClient.shutdown();
    logger.info("cloudwatch client stopped");
  }

}
