package fb.risk;

import com.google.protobuf.ByteString;
import io.grpc.*;

import java.nio.ByteBuffer;
import java.util.List;

public class FlatDBGrpcClient {
    public static void main(String[] args) throws Exception {
        ManagedChannel channel = Grpc.newChannelBuilder("localhost:50051", InsecureChannelCredentials.create())
                .build();

        MethodDescriptor<List<String>, ByteString> method = MethodDescriptor.<List<String>, ByteString>newBuilder()
                .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                .setFullMethodName("fb.risk.SymbolService/GetSymbols")
                .setRequestMarshaller(new ListStringMarshaller())
                .setResponseMarshaller(new ByteStringMarshaller())
                .build();

        ClientCall<List<String>, ByteString> call = channel.newCall(method, CallOptions.DEFAULT);

        call.start(new ClientCall.Listener<>() {
            @Override
            public void onMessage(ByteString message) {
                ByteBuffer bb = message.asReadOnlyByteBuffer().order(java.nio.ByteOrder.LITTLE_ENDIAN);
                fb.risk.SymbolData sym = fb.risk.SymbolData.getRootAsSymbolData(bb);
                System.out.println("Received symbol: " + sym.symbol());
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                System.out.println("Stream closed: " + status);
            }
        }, new Metadata());

        call.sendMessage(List.of("AAPL", "MSFT"));
        call.halfClose();

        Thread.sleep(2000); // wait for stream to complete
        channel.shutdownNow();
    }
}

