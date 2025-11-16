package fb.risk;

import com.google.protobuf.ByteString;
import io.grpc.MethodDescriptor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class ByteStringMarshaller implements MethodDescriptor.Marshaller<ByteString> {

    @Override
    public InputStream stream(ByteString value) {
        return new ByteArrayInputStream(value.toByteArray());
    }

    @Override
    public ByteString parse(InputStream stream) {
        try {
            return ByteString.readFrom(stream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

