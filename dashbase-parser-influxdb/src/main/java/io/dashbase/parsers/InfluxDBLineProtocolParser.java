package io.dashbase.parsers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.Clock;
import java.util.Map;

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
  
  private void extractNVPairs(String tagsString, Map<String,String> content, String type) {
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
  
	@Override
	public boolean parse(byte[] rawContent,@NotNull RapidIngestedData data,
			io.dashbase.ingester.DashbaseIngesterParser.ParseContext ctx, Clock clock) throws Exception {
	  
	  // we don't save raw content for influx metrics data
	  data.payload = EMPTY;
	  
    data.content.clear();
    String line = new String(rawContent, Charsets.UTF_8);
    
    String[] parts = line.split(" ");
    
    if (parts.length != 3) {
      return false;
    }
    
    int idx = parts[0].indexOf(",");
    if (idx <0) {
      return false;
    }
    String name = parts[0].substring(0, idx);
    data.content.put("_name",name);
    RapidColumn nameCol = new RapidColumn();
    nameCol.setType(RapidColumn.META_TYPE);
    schemaMap.put("_name", nameCol);
    extractNVPairs(parts[0].substring(idx+1), data.content, RapidColumn.META_TYPE);
    extractNVPairs(parts[1], data.content, RapidColumn.NUMERIC_TYPE);
    data.timeStampInMillis = timeParser.parseTimestamp(parts[2]);    
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
      while(count<numToRead) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        parser.parse(line.getBytes(Charsets.UTF_8), data, parseCtx, Clock.systemUTC());
        System.out.println("data: " + data);
        System.out.println("schema: " + parser.getSchema());
        System.out.println("========================================");
        count++;
      }
    }
  }

}
