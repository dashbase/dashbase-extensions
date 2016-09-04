package com.qiniu.dashbase.firehose.proxy;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;
import static spark.Spark.stop;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rapid.firehose.RapidFirehose;
import rapid.firehose.RapidFirehoseMessage;
import rapid.server.config.Configurable;
import rapid.server.config.Measurable;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
public class HttpFirehose extends RapidFirehose implements Configurable, Measurable {
    private static Logger logger = LoggerFactory.getLogger(HttpFirehose.class);
    private int count = 0;
    private BlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(1050000);
    private byte[] nextData;
    private static final  char doubleQuote='"';
    private HashSet<String> keys = new HashSet<>();
    
    private Meter blockMeter = null;

    @Override
    public void seekToOffset(String offset) {
        System.out.println("can't support seek");
    }

    @Override
    public boolean isDrained() {

        return false;
    }

    @Override
    public Iterator<RapidFirehoseMessage> iterator() {
        return new Iterator<RapidFirehoseMessage>() {

            @Override
            public boolean hasNext() {

                return true;
            }

            @Override
            public RapidFirehoseMessage next() {

                try {
                    nextData = queue.take();
                    count--;
                } catch (InterruptedException e) {
                    System.out.println("is Drained error-.-");
                    e.printStackTrace();
                }
                RapidFirehoseMessage msg = new RapidFirehoseMessage() {
                    @Override
                    public String offset() {
                        System.out.println("offset");
                        return String.valueOf(count);
                    }

                    @Override
                    public byte[] data() {
                        return nextData;
                    }
                };

                return msg;
            }

        };
    }

    @Override
    public void start() throws Exception {
        logger.info("start");
        System.out.println("Start");
    }

    @Override
    public void shutdown() throws Exception {
        stop();
    }

    @Override
    public void configure(Map<String, Object> params) {
        logger.info("start proxy v1.0.1-alpha server ");
        int port = 9999;
        if (params != null && params.containsKey("port")) {
            port = Integer.parseInt((String) params.get("port"));
        }
        if (params != null && params.containsKey("keys")) {
            List _keys = (List) params.get("keys");
            for (Object _key : _keys) {
                keys.add((String) _key);
            }

        }
        runServer(port);
    }


    private void runServer(int port) {

        port(port);
        post("/insert", (request, response) -> {
            if (count >= 1000000) {
                response.status(403);
                logger.info("BLOCK");
                blockMeter.mark();
                return "block";
            }

            //  data format:
            //      a=10  c=20    b=30
            //      a=40  b=39    c=12
            //  event:  <key>=<value>\t<key>=<value>\n

            String[] datas = request.body().split("\n");
            String[] tmp;
            String key;
            String value;
            logger.debug(String.format("Data Count %d", datas.length));

            for (String data : datas) {
                String[] KVs = data.split("\t");
                String message = "{";
                for (String KV : KVs) {
                    tmp = KV.split("=", 2);
                    key = tmp[0];
                    value = tmp[1];
                    if (keys.contains(key)) {
                        message += doubleQuote + key + doubleQuote + ":";
                        message += jsonEscape(value);
                        message += ",";
                    }
                }
                //  remove the last quote
                message = message.substring(0, message.length() - 1);
                message += "}";
                queue.add(message.getBytes());
                count++;
            }


            return "ok";
        });
        get("/ping", (request, response) -> "pong");

    }

    private static String jsonEscape(String string) {
        if (string == null || string.length() == 0) {
            return "\"\"";
        }

        char         c = 0;
        int          i;
        int          len = string.length();
        StringBuilder sb = new StringBuilder(len + 4);
        String       t;

        sb.append('"');
        for (i = 0; i < len; i += 1) {
            c = string.charAt(i);
            switch (c) {
                case '\\':
                case '"':
                    sb.append('\\');
                    sb.append(c);
                    break;
                case '/':
                    //                if (b == '<') {
                    sb.append('\\');
                    //                }
                    sb.append(c);
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                default:
                    if (c < ' ') {
                        t = "000" + Integer.toHexString(c);
                        sb.append("\\u" + t.substring(t.length() - 4));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
        return sb.toString();
    }

	@Override
	public void registerMetrics(MetricRegistry metricRegistry) {
		metricRegistry.register("httpproxy.firehose.queue.size", (Gauge<Integer>) () -> queue.size());
		blockMeter = metricRegistry.meter("httpproxy.firehose.block");
	}

}

