package io.dashbase.parsers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.Clock;
import java.util.Map;
import java.util.StringTokenizer;

import javax.validation.constraints.NotNull;

import org.apache.lucene.util.BytesRef;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

import rapid.ingester.RapidIngestedData;
import rapid.ingester.RapidIngesterParser;
import rapid.parser.EpochTimeValueParser;
import rapid.parser.RapidColumn;

public class InfluxDBLineProtocolParser implements RapidIngesterParser {

  private EpochTimeValueParser timeParser = new EpochTimeValueParser();
  private static BytesRef EMPTY = new BytesRef(new byte[0]);
  private Map<String, RapidColumn> schemaMap = Maps.newHashMap();

  private void extractNVPairs(String tagsString, Map<String, String> content, String type) {
    String[] parts = tagsString.split(",");
    for (String part : parts) {
      String[] nvPair = part.split("=");
      String name = nvPair[0];
      String val = nvPair[1];
      if (RapidColumn.NUMERIC_TYPE.equals(type) && val.endsWith("i")) {
        val = val.substring(0, val.length() - 1);
      }
      content.put(name, val);
      if (!schemaMap.containsKey(name)) {
        RapidColumn col = new RapidColumn();
        col.name = name;
        col.setType(type);
        schemaMap.put(name, col);
      }
    }
  }
  
  private void addTagNVPair(String pair, Map<String, String> content) {
    StringTokenizer strtok = new StringTokenizer(pair, "=");
    String name = strtok.nextToken();
    String val = strtok.nextToken();
    if (val.endsWith("i")) {
      val = val.substring(0, val.length() - 1);
    }
    
    content.put(name, val);
    if (!schemaMap.containsKey(name)) {
      RapidColumn col = new RapidColumn();
      col.name = name;
      col.setType(RapidColumn.META_TYPE);
      schemaMap.put(name, col);
    }
  }
  
  private void addNumericNVPair(String pair, Map<String, String> content) {
    StringTokenizer strtok = new StringTokenizer(pair, "=");
    String name = strtok.nextToken();
    String val = strtok.nextToken();
    
    content.put(name, val);
    if (!schemaMap.containsKey(name)) {
      RapidColumn col = new RapidColumn();
      col.name = name;
      col.setType(RapidColumn.NUMERIC_TYPE);
      schemaMap.put(name, col);
    }
  }

  @Override
  public boolean parse(byte[] rawContent, @NotNull RapidIngestedData data,
      io.dashbase.ingester.DashbaseIngesterParser.ParseContext ctx, Clock clock) throws Exception {

    // we don't save raw content for influx metrics data
    data.payload = EMPTY;

    data.content.clear();
    String line = new String(rawContent, Charsets.UTF_8);
    
    StringTokenizer strTok = new StringTokenizer(line, " ");
    
    String tagsLine = strTok.nextToken();
    StringTokenizer tagsTok = new StringTokenizer(tagsLine, ",");
    
    String name = tagsTok.nextToken();
    data.content.put("_name", name);
    RapidColumn nameCol = new RapidColumn();
    nameCol.setType(RapidColumn.META_TYPE);
    schemaMap.put("_name", nameCol);
    
    String nvPair;
    
    while(tagsTok.hasMoreTokens()) {
      nvPair = tagsTok.nextToken();
      addTagNVPair(nvPair, data.content);
    }
    
    
    String numericsLine = strTok.nextToken();
    StringTokenizer numTok = new StringTokenizer(numericsLine, ",");
    
    while(numTok.hasMoreTokens()) {
      nvPair = numTok.nextToken();
      addNumericNVPair(nvPair, data.content);
    }
    
    String timeVal = strTok.nextToken();
    data.timeStampInMillis = timeParser.parseTimestamp(timeVal);
    return true;
  }

  @Override
  public Map<String, RapidColumn> getSchema() {
    return schemaMap;
  }

  public static void main(String[] args) throws Exception {
    int numToRead = 10;
    File f = new File("/Users/john/github/influxdb-comparisons/bulkout.txt");
    int count = 0;
    InfluxDBLineProtocolParser parser = new InfluxDBLineProtocolParser();
    RapidIngestedData data = new RapidIngestedData();
    ParseContext parseCtx = new ParseContext();
    try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
      while (count < numToRead) {
        String line = reader.readLine();        
        if (line == null) {
          break;
        }
        System.out.println(line);
        parser.parse(line.getBytes(Charsets.UTF_8), data, parseCtx, Clock.systemUTC());
        System.out.println("data: " + data);
        System.out.println("schema: " + parser.getSchema());
        System.out.println("========================================");
        count++;
      }
    }
  }

}
