package fb.risk;

import io.grpc.MethodDescriptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class FlatBufferMarshaller implements MethodDescriptor.Marshaller<ByteBuffer> {

    @Override
    public InputStream stream(ByteBuffer buf) {
        // Wrap the ByteBuffer as InputStream without copying
        ByteBuffer readOnly = buf.asReadOnlyBuffer();
        return new InputStream() {
            @Override
            public int read() {
                if (!readOnly.hasRemaining()) return -1;
                return readOnly.get() & 0xFF;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                if (!readOnly.hasRemaining()) return -1;
                len = Math.min(len, readOnly.remaining());
                readOnly.get(b, off, len);
                return len;
            }
        };
    }

    @Override
    public ByteBuffer parse(InputStream stream) {
        try {
            byte[] data = stream.readAllBytes();
            return ByteBuffer.wrap(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

