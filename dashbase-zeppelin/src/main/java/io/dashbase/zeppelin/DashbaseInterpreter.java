package io.dashbase.zeppelin;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;

import io.dashbase.zeppelin.util.DashbaseInterpreterUtil;
import rapid.api.RapidRequest;
import rapid.api.RapidResponse;
import rapid.api.RapidServiceInfo;
import rapid.api.query.StringQuery;

public class DashbaseInterpreter extends Interpreter {
  
  private static Logger logger = LoggerFactory.getLogger(DashbaseInterpreter.class);
  
  private static final int DEFAULT_CONNECTION_TIMEOUT = 10000;
  private static final int DEFAULT_SOCKET_TIMEOUT = 60000;
  
  private static final String QUERY_URI = "/query";
  private static final String SQL_URI = "/sql";
  private static final String GET_INFO_URI = "/get-info";
  
  private static final String HELP = "Dashbase interpreter:\n"
      + "General format: <command> <option>\n"
      + "Commands:\n"
      + "  - help \n"
      + "    . this message\n"
      + "  - info\n"
      + "    . shows schema information\n"
      + "  - search <query>\n"
      + "    . performs full text query\n"
      + "  - sql <sql_statement>\n"
      + "    . performs SQL query";
  
  private String dashbaseUrl = null;
  private String queryUrl;
  private String sqlUrl;
  private String getInfoUrl;  

  private final ObjectMapper objMapper = new ObjectMapper();  
  
  protected static final List<String> COMMANDS = Arrays.asList(
      "tables", "info", "sql", "help", "search");
  
  static final String CLIENT_NAME = "dashbase_zeppelin";
  
  public static final String DASHBASE_URL = "dashbase.url";
  public static final String DASHBASE_CONNECTION_TIMEOUT = "dashbase.connection.timeout";
  public static final String DASHBASE_SOCKET_TIMEOUT = "dashbase.socket.timeout";
  
	public DashbaseInterpreter(Properties property) {
    super(property);
  }		

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor) {
    final List<InterpreterCompletion> suggestions = new ArrayList<>();

    for (final String cmd : COMMANDS) {
      if (cmd.toLowerCase().contains(buf)) {
        suggestions.add(new InterpreterCompletion(cmd, cmd));
      }
    }
    return suggestions;
  }



  @Override
	public void cancel(InterpreterContext interpreterContext) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
	  logger.info("shutting down dashbase interpreter");
	  try {
      Unirest.shutdown();
    } catch (IOException e) {
      logger.error("problem shutting unirest", e);
    }
	  logger.info("dashbase interpreter shutdown");
	}

	@Override
	public FormType getFormType() {
		return FormType.SIMPLE;
	}

	@Override
	public int getProgress(InterpreterContext interpreterContext) {	  
		// TODO Auto-generated method stub
		return 0;
	}
	
	private InterpreterResult handleGetInfo() {
	  try {
	    
	    HttpResponse<InputStream> response = Unirest.post(getInfoUrl)
        .header(HttpHeaders.CONTENT_TYPE,"application/json; charset=utf-8").asBinary();
      RapidServiceInfo info =  objMapper.readValue(response.getBody(), RapidServiceInfo.class);
      return DashbaseInterpreterUtil.toInterpretedGetInfo(info);
	  } catch(Exception e) {
      return DashbaseInterpreterUtil.exception(e);
    }  
	}
	
	private InterpreterResult handleSearch(String query, InterpreterContext interpreterContext) {
	  
	  RapidRequest req = new RapidRequest();
	  req.query = new StringQuery(query);
	  req.numResults = 10;
	  
	  try {
	    ObjectMapper mapper = new ObjectMapper();
	    String queryJson = mapper.writeValueAsString(req);
      HttpResponse<InputStream> response = Unirest.post(queryUrl)
        .header(HttpHeaders.CONTENT_TYPE,"application/json; charset=utf-8").body(queryJson).asBinary();    
      RapidResponse resp =  objMapper.readValue(response.getBody(), RapidResponse.class);
      return DashbaseInterpreterUtil.toInterpretedSearchResult(resp);
    } catch(Exception e) {
      return DashbaseInterpreterUtil.exception(e);
    }
	}
	
	private InterpreterResult handleSql(String sql, InterpreterContext interpreterContext) {
	  if (sql == null || sql.trim().length() == 0) {
	    return new InterpreterResult(InterpreterResult.Code.SUCCESS);
	  }
	  try {
	    HttpResponse<InputStream> response = Unirest.post(sqlUrl)
        .header(HttpHeaders.CONTENT_TYPE,"application/json; charset=utf-8").body(sql).asBinary();    
      RapidResponse resp =  objMapper.readValue(response.getBody(), RapidResponse.class);
      return DashbaseInterpreterUtil.toInterpretedSqlResult(resp);
	  } catch(Exception e) {
	    return DashbaseInterpreterUtil.exception(e);
	  }    
	}	

	@Override
	public InterpreterResult interpret(String cmd, InterpreterContext interpreterContext) {
	  if (StringUtils.isEmpty(cmd) || StringUtils.isEmpty(cmd.trim())) {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS);
    }	  

    //int currentResultSize = resultSize;

    if (dashbaseUrl == null) {
      return new InterpreterResult(InterpreterResult.Code.ERROR,
        "Dashbase url is not configured.");
    }

    String[] items = StringUtils.split(cmd.trim(), " ", 3);
    String command = items[0];
    // Process some specific commands (help, size, ...)
    if ("sql".equalsIgnoreCase(command)) {
      return handleSql(DashbaseInterpreterUtil.concatArgs(items), interpreterContext);
    }
    if ("info".equalsIgnoreCase(command)) {
      return handleGetInfo();
    }
    if ("search".equalsIgnoreCase(command)) {
      return handleSearch(DashbaseInterpreterUtil.concatArgs(items), interpreterContext);
    }
    if ("help".equalsIgnoreCase(command)) {
      return processHelp(InterpreterResult.Code.SUCCESS, null);
    }
    
	  return processHelp(
        InterpreterResult.Code.ERROR,
        "unsupported command: " + command);
	}
	
	private InterpreterResult processHelp(InterpreterResult.Code code, String additionalMessage) {
    final StringBuffer buffer = new StringBuffer();

    if (additionalMessage != null) {
      buffer.append(additionalMessage).append("\n");
    }

    buffer.append(HELP).append("\n");

    return new InterpreterResult(code, InterpreterResult.Type.TEXT, buffer.toString());
  }

	@Override
	public void open() {
	  logger.info("starting dashbase interpreter");
	  dashbaseUrl = getProperty(DASHBASE_URL);
	  if (!dashbaseUrl.startsWith("http://")) {
	    StringBuilder buf = new StringBuilder();
	    buf.append("http://").append(dashbaseUrl);
	    dashbaseUrl = buf.toString();
	  }
	  
	  long connectionTimeout;
	  try {
	    connectionTimeout = Long.parseLong(getProperty(DASHBASE_CONNECTION_TIMEOUT));
	  } catch(Exception e) {
	    connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
	  }
	  
	  long socketTimeout;
    try {
      socketTimeout = Long.parseLong(getProperty(DASHBASE_SOCKET_TIMEOUT));
    } catch(Exception e) {
      socketTimeout = DEFAULT_SOCKET_TIMEOUT;
    }
	  
	  Unirest.setTimeouts(connectionTimeout, socketTimeout);	  
    
    queryUrl = dashbaseUrl + QUERY_URI;
    getInfoUrl = dashbaseUrl + GET_INFO_URI;
    sqlUrl = dashbaseUrl + SQL_URI;
	  logger.info("dashbase interpreter started");
	}
}
