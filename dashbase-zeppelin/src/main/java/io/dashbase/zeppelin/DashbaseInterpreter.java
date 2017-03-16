package io.dashbase.zeppelin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dashbase.client.http.HttpClientService;
import io.dashbase.zeppelin.util.DashbaseInterpreterUtil;
import rapid.api.RapidResponse;
import rapid.api.RapidServiceInfo;
import retrofit2.Call;
import retrofit2.Response;

public class DashbaseInterpreter extends Interpreter {
  
  private static Logger logger = LoggerFactory.getLogger(DashbaseInterpreter.class);
  
  private static final int DEFAULT_CONNECTION_TIMEOUT = 10000;
  private static final int DEFAULT_SOCKET_TIMEOUT = 60000;
  
  private HttpClientService svc = null;
  
  private static final String HELP = "Dashbase interpreter:\n"
      + "General format: <command> <option>\n"
      + "Commands:\n"
      + "  - help \n"
      + "    . this message\n"
      + "  - info <table1,table2...>\n"
      + "    . shows schema information\n"
      + "  - search <query>\n"
      + "    . performs full text query\n"
      + "  - sql <sql_statement>\n"
      + "    . performs SQL query";
  
  private String dashbaseUrl = null;
  
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
	  logger.info("dashbase interpreter shutdown");
	}

	@Override
	public FormType getFormType() {
		return FormType.SIMPLE;
	}

	@Override
	public int getProgress(InterpreterContext interpreterContext) {
		return 0;
	}
	
	private InterpreterResult handleGetInfo(List<String> names) {
	  try {
      RapidServiceInfo info =  svc.query().getInfo(names).execute().body();
      return DashbaseInterpreterUtil.toInterpretedGetInfo(info);
	  } catch(Exception e) {
      return DashbaseInterpreterUtil.exception(e);
    }  
	}
	
	private InterpreterResult handleSearch(String name, String query, InterpreterContext interpreterContext) {	  
	  try {	    
	    Call<RapidResponse> callResp = svc.query().search(name, query, 10);
	    Response<RapidResponse> rapidResp = callResp.execute();
	    logger.info(String.valueOf(rapidResp.message()));
      RapidResponse resp =  rapidResp.body();
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
      RapidResponse resp = svc.query().sql(sql).execute().body();
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
      String nameList = DashbaseInterpreterUtil.concatArgs(items);
      List<String> names = nameList == null ? Collections.emptyList() : 
        Arrays.asList(nameList.split(","));
      return handleGetInfo(names);
    }
    if ("search".equalsIgnoreCase(command)) {
      String fullargs = DashbaseInterpreterUtil.concatArgs(items);
      int idx = fullargs.indexOf(" ");
      String name = null;
      String query = null;
      if (idx > 0) {
        name = fullargs.substring(0, idx);
        query = fullargs.substring(idx + 1, fullargs.length());
      } else {
        name = null;
        query = fullargs;
      }
      return handleSearch(name, query, interpreterContext);
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
      
      svc = new HttpClientService(dashbaseUrl);
	  logger.info("dashbase interpreter started");
	}	
}
