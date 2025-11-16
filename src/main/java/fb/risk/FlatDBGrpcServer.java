package fb.risk;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public class FlatDBGrpcServer {

    private final FlatDBReader reader;
    private final Server server;

    public FlatDBGrpcServer(FlatDBReader reader, int port) throws Exception {
        this.reader = reader;

        MethodDescriptor<List<String>, ByteString> method =
                MethodDescriptor.<List<String>, ByteString>newBuilder()
                        .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                        .setFullMethodName("fb.risk.SymbolService/GetSymbols")
                        .setRequestMarshaller(new ListStringMarshaller())
                        .setResponseMarshaller(new ByteStringMarshaller())
                        .build();

        ServerServiceDefinition service = ServerServiceDefinition.builder("fb.risk.SymbolService")
                .addMethod(method, ServerCalls.asyncServerStreamingCall(this::getSymbols))
                .build();

        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(service)
                .build()
                .start();
        System.out.println("Server started on port " + port);
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    private void getSymbols(List<String> symbols, StreamObserver<ByteString> responseObserver) {
        for (String sym : symbols) {
            Optional<ByteBuffer> sliceOpt = reader.sliceFor(sym);
            if (sliceOpt.isPresent()) {
                ByteBuffer slice = sliceOpt.get();
                ByteString payload = com.google.protobuf.UnsafeByteOperations.unsafeWrap(slice);
                System.out.println("Streaming symbol: " + sym + " size=" + slice.remaining());
                responseObserver.onNext(payload);
            } else {
                System.out.println("Symbol not found: " + sym);
            }
        }
        responseObserver.onCompleted();
    }
}

