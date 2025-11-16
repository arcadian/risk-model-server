package fb.risk;

import io.grpc.*;
import java.nio.ByteBuffer;
import java.util.List;

public class FlatDBGrpcClient {

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = Grpc.newChannelBuilder("localhost:50051", InsecureChannelCredentials.create())
                .build();

        MethodDescriptor<List<String>, ByteBuffer> method = // same as server
            MethodDescriptor.<List<String>, ByteBuffer>newBuilder()
                .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                .setFullMethodName("fb.risk.SymbolService/GetSymbols")
                .setRequestMarshaller(new ListStringMarshaller())
                .setResponseMarshaller(new FlatBufferMarshaller())
                .build();

        ClientCall<List<String>, ByteBuffer> call = channel.newCall(method, CallOptions.DEFAULT);
        call.start(new ClientCall.Listener<>() {
            @Override
            public void onMessage(ByteBuffer message) {
                fb.risk.SymbolData sym = fb.risk.SymbolData.getRootAsSymbolData(message);
                System.out.println("Received symbol: " + sym.symbol());
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                System.out.println("Stream closed: " + status);
            }
        }, new Metadata());

        call.sendMessage(List.of("AAPL", "MSFT"));
        call.halfClose();
        channel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
    }
}

