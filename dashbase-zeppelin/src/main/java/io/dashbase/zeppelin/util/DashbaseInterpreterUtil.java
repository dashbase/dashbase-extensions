package io.dashbase.zeppelin.util;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResultMessage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import rapid.api.AggregationResponse;
import rapid.api.Facet;
import rapid.api.FacetAggregationResponse;
import rapid.api.HistogramAggregationResponse;
import rapid.api.HistogramBucket;
import rapid.api.NumericAggregationResponse;
import rapid.api.RapidField;
import rapid.api.RapidHit;
import rapid.api.RapidPayload;
import rapid.api.RapidResponse;
import rapid.api.RapidServiceInfo;

public class DashbaseInterpreterUtil {
  private static ObjectMapper mapper = new ObjectMapper();
  private DashbaseInterpreterUtil() {}  
  
  public static String concatArgs(String[] items) {
    if (items.length <= 1) {
      return null;
    } else {
      StringBuilder buf = new StringBuilder();
      for (int i = 1; i < items.length; ++i) {
        buf.append(items[i]).append(" ");
      }
      return buf.toString();
    }
  }
  
  public static InterpreterResult exception(Exception e) {
    return new InterpreterResult(InterpreterResult.Code.ERROR,
        "Encountered exception: " + ((e == null) ? "unknown" : e.getMessage()));
  }
  
  public static InterpreterResult toInterpretedSearchResult(RapidResponse resp) {    
    if (resp == null) {
      return new InterpreterResult(InterpreterResult.Code.ERROR,
          "Null Dashbase response");
    }
    List<InterpreterResultMessage> msgList = new LinkedList<>();
    msgList.add(renderStats(resp));
    
    InterpreterResultMessage hitsMessage = renderHits(resp.hits);
    if (hitsMessage != null) {
      msgList.add(hitsMessage);
    }
    
    return new InterpreterResult(
        InterpreterResult.Code.SUCCESS,
        msgList);
  
  }
  
  public static InterpreterResult toInterpretedSqlResult(RapidResponse resp) {
    try {
      if (resp == null) {
        return new InterpreterResult(InterpreterResult.Code.ERROR,
            "Null Dashbase response");
      }
      List<InterpreterResultMessage> msgList = new LinkedList<>();
      
      msgList.add(renderStats(resp));
      
      for (Entry<String, AggregationResponse> entry: resp.aggregations.entrySet()) {
        AggregationResponse aggrResp = entry.getValue();
        if (aggrResp instanceof FacetAggregationResponse) {
          msgList.add(renderTopNAggregation((FacetAggregationResponse)aggrResp));
        } else if (aggrResp instanceof HistogramAggregationResponse) {
          msgList.add(renderTSAggregation((HistogramAggregationResponse)aggrResp));
        } else if (aggrResp instanceof NumericAggregationResponse) {
          msgList.add(renderNumericAggregation(entry.getKey(), (NumericAggregationResponse)aggrResp));
        }
      }
      
      InterpreterResultMessage hitsMessage = renderHits(resp.hits);
      if (hitsMessage != null) {
        msgList.add(hitsMessage);
      }
      
      String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(resp);
      
      InterpreterResultMessage jsonMessage = new InterpreterResultMessage(InterpreterResult.Type.TEXT,
          json);
      
      msgList.add(jsonMessage);
      
      
      return new InterpreterResult(
          InterpreterResult.Code.SUCCESS,
          msgList);
    } catch (JsonProcessingException e) {
      return new InterpreterResult(InterpreterResult.Code.ERROR,
          "Unable serialize Dashbase response: " + ((e == null) ? "unknown" : e.getMessage()));
    }
  }
  
  public static InterpreterResult toInterpretedGetInfo(RapidServiceInfo info) {
    if (info == null) {
      return new InterpreterResult(InterpreterResult.Code.ERROR,
          "Null Dashbase Info");
    }
    List<InterpreterResultMessage> msgList = new LinkedList<>();
    msgList.add(new InterpreterResultMessage(InterpreterResult.Type.TEXT, "number of events: " + info.numDocs));
    
    StringBuilder buff = new StringBuilder();
    buff.append("name").append("\t")          
        .append("meta").append("\t")
        .append("numeric").append("\t")
        .append("searchable").append("\n");
    for (RapidField field : info.schema.fields) {
      buff.append(field.name).append("\t")        
      .append(field.isMeta).append("\t")
      .append(field.isNumeric).append("\t")
      .append(field.isSearchable).append("\n");
    }
    
    msgList.add(new InterpreterResultMessage(InterpreterResult.Type.TABLE, buff.toString()));
    
    return new InterpreterResult(
        InterpreterResult.Code.SUCCESS,          
        msgList);
  }
  
  public static InterpreterResultMessage renderStats(RapidResponse resp) {
    
    DecimalFormat formatter = new DecimalFormat("###,###");
    StringBuilder buff = new StringBuilder();
    
    buff.append("Found : <b>").append(formatter.format(resp.numHits))
    .append("</b> out of ").append(formatter.format(resp.totalDocs)).append(" events, took ")
    .append(formatter.format(resp.latencyInMillis)).append(" milliseconds");
    
    return new InterpreterResultMessage(InterpreterResult.Type.HTML, buff.toString());
  }
  
  public static InterpreterResultMessage renderTopNAggregation(FacetAggregationResponse aggreResp) {
    StringBuilder buff = new StringBuilder();
    buff.append("value").append("\t")          
    .append("count").append("\n");
    for (Facet  facet : aggreResp.facets) {
      buff.append(facet.value).append("\t")        
      .append(facet.count).append("\n");      
    }
    return new InterpreterResultMessage(InterpreterResult.Type.TABLE, buff.toString());
  }
  
  // maps bucket size in seconds, the indicator of time granularity to corresponding 
  // date formatter
  
  static final Map<Long, DateFormat> DATE_FORMAT_MAP = ImmutableMap.of(
      3600L, new SimpleDateFormat("MM/dd/yyyy h:mm a"),  // hour
      86400L, new SimpleDateFormat("MM/dd/yyyy")     // day
  );
  
  public static InterpreterResultMessage renderTSAggregation(HistogramAggregationResponse aggreResp) {
    DateFormat df = DATE_FORMAT_MAP.get(new Long(aggreResp.bucketSizeInSeconds));
    
    // default to seconds
    if (df == null) {
      df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    }
    
    StringBuilder buff = new StringBuilder();
    buff.append("seconds").append("\t")
    .append("count").append("\n");
    for (HistogramBucket  bucket : aggreResp.histogramBuckets) {      
      buff.append(df.format(new Date((long)bucket.timeInSec * 1000L))).append("\t")        
      .append(bucket.count).append("\n");   
    }
    return new InterpreterResultMessage(InterpreterResult.Type.TABLE, buff.toString());
  }
  
  public static InterpreterResultMessage renderNumericAggregation(String name, NumericAggregationResponse aggreResp) {
    DecimalFormat formatter = new DecimalFormat("###,###.##");
    StringBuilder buff = new StringBuilder();
    buff.append("<p>").append(name).append("</p>");
    buff.append("<p><h1>").append(formatter.format(aggreResp.value)).append("</h1></p>");
    return new InterpreterResultMessage(InterpreterResult.Type.HTML, buff.toString());
  }
  
  public static InterpreterResultMessage renderHits(List<RapidHit> hits) {
    if (hits == null || hits.isEmpty()) {
      return null;
    }
    
    DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    
    LinkedHashSet<String> fieldNames = new LinkedHashSet<>();
    for (RapidHit hit : hits) {
      if (hit.getPayload() != null) {
        fieldNames.addAll(hit.getPayload().fields.keySet());
      }
    }
    
    StringBuilder buff = new StringBuilder();
    buff.append("time(s)").append("\t");
    for (String fieldName : fieldNames) {
      buff.append(fieldName).append("\t");  
    }    
    buff.append("raw").append("\n");
    
    for (RapidHit hit : hits) {      
      buff.append(df.format(new Date(hit.getTimeInSeconds() * 1000))).append("\t");
      RapidPayload payload = hit.getPayload();
      for (String fieldName : fieldNames) {
        List<String> fieldVals = payload.fields.get(fieldName);
        if (fieldVals == null || fieldVals.isEmpty()) {
          buff.append("").append("\t");
        } else if (fieldVals.size() == 1) {
          buff.append(fieldVals.get(0)).append("\t");
        } else {
          buff.append(fieldVals).append("\t");
        }
      }
      buff.append(payload.stored).append("\n");
    }
    
    return new InterpreterResultMessage(
        InterpreterResult.Type.TABLE,          
        buff.toString());
  }
}
