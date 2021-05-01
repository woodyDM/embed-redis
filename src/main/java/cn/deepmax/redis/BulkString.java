package cn.deepmax.redis;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author wudi
 * @date 2021/4/29
 */
public class BulkString {
    private List<String> msg;

    public BulkString(List<String> msg) {
        this.msg = msg;
    }

    public void writeTo(ByteBuf byteBuf) {
        StringBuilder sb = new StringBuilder();
        sb.append("$");

        if (msg == null) {
            sb.append(-1)
                    .append(Constants.EOL);
        } else if (msg.size() == 0) {
            sb.append(0)
                    .append(Constants.EOL)
                    .append(Constants.EOL);
        }
        for (String it : msg) {
            sb.append(it).append(Constants.EOL);
        }
        byteBuf.writeBytes(sb.toString().getBytes(StandardCharsets.UTF_8));
    }
}
