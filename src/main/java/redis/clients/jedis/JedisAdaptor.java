package redis.clients.jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Just an adaptor for Jedis that allows the generic way of executing a Command
 */
public class JedisAdaptor {

    // translation from string to Command ref
    private final static Map<String, Protocol.Command> commands = new HashMap<String, Protocol.Command>() {{
        put("lrange", Protocol.Command.LRANGE);
        put("smembers", Protocol.Command.SMEMBERS);
        put("zrange", Protocol.Command.ZRANGE);
        put("zrangebyscore", Protocol.Command.ZRANGEBYSCORE);
    }};

    public static List<Object> execute(String url, String command, String[] args) {
        Jedis jedis = new Jedis(url);
        Protocol.Command cmd = parseCommand(command);
        Connection conn = jedis.client.sendCommand(cmd, args);
        return (List) conn.getMultiBulkReply();
    }

    private static Protocol.Command parseCommand(String command) {
        Protocol.Command cmd = commands.get(command);
        if (cmd == null)
            throw new RuntimeException("Command [" + command + "] not supported");
        return cmd;
    }

}
