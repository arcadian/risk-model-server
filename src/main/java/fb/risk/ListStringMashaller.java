package fb.risk;

import io.grpc.MethodDescriptor;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ListStringMarshaller implements MethodDescriptor.Marshaller<List<String>> {

    @Override
    public InputStream stream(List<String> value) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(value.size());
            for (String s : value) {
                byte[] b = s.getBytes("UTF-8");
                dos.writeInt(b.length);
                dos.write(b);
            }
            return new ByteArrayInputStream(out.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> parse(InputStream stream) {
        try {
            DataInputStream dis = new DataInputStream(stream);
            int n = dis.readInt();
            List<String> result = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                int len = dis.readInt();
                byte[] b = new byte[len];
                dis.readFully(b);
                result.add(new String(b, "UTF-8"));
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

