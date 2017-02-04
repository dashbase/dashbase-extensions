package io.dashbase.firehose.cloudwatch;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rapid.firehose.RapidFirehose;
import rapid.server.config.Configurable;

import com.amazonaws.services.logs.*;


public class CloudWatchFirehose implements RapidFirehose, Configurable {
  private static Logger logger = LoggerFactory.getLogger(CloudWatchFirehose.class);
  private static ObjectMapper mapper = new ObjectMapper();

  private GetLogEventsResult result;
  private Iterator<OutputLogEvent> eventsIterator;
  private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

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
        result = result.withNextForwardToken(fwdToken);
        eventsIterator = result.getEvents().iterator();

        logger.info("started another batch of 10k events");
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
    logger.info("start dashbase cloudwatch firehose server ");

    String logGroupName = Preconditions.checkNotNull((String) params.get("log_group_name"));
    String logStreamName = Preconditions.checkNotNull((String) params.get("log_stream_name"));

    AWSLogs cloudWatchClient = AWSLogsClientBuilder.defaultClient();

    result = cloudWatchClient.getLogEvents(new GetLogEventsRequest()
      .withLogGroupName(logGroupName)
      .withLogStreamName(logStreamName));

    eventsIterator = result.getEvents().iterator();
  }
}
