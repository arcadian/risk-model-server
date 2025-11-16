package fb.risk;

import io.grpc.MethodDescriptor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.InputStream;

public class ByteBufMarshaller implements MethodDescriptor.Marshaller<ByteBuf> {

    @Override
    public InputStream stream(ByteBuf buf) {
        return new InputStream() {
            @Override
            public int read() {
                return buf.isReadable() ? (buf.readByte() & 0xFF) : -1;
            }

            @Override
            public int read(byte[] dst, int off, int len) {
                int toRead = Math.min(len, buf.readableBytes());
                if (toRead == 0) return -1;
                buf.readBytes(dst, off, toRead);
                return toRead;
            }
        };
    }

    @Override
    public ByteBuf parse(InputStream is) {
        // For requests only (small strings)
        ByteBuf out = Unpooled.buffer();
        try {
            byte[] tmp = new byte[128];
            int n;
            while ((n = is.read(tmp)) != -1) {
                out.writeBytes(tmp, 0, n);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out;
    }
}

