package fb.risk;

import io.grpc.*;
import io.grpc.stub.ServerCalls;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

public class FlatDBGrpcServer {

    private final FlatDBReader reader;

    public FlatDBGrpcServer(FlatDBReader reader) {
        this.reader = reader;
    }

    public Server start(int port) {
        MethodDescriptor<List<String>, ByteBuffer> method =
                MethodDescriptor.<List<String>, ByteBuffer>newBuilder()
                        .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                        .setFullMethodName("fb.risk.SymbolService/GetSymbols")
                        .setRequestMarshaller(new ListStringMarshaller())
                        .setResponseMarshaller(new FlatBufferMarshaller())
                        .build();

        ServerServiceDefinition service = ServerServiceDefinition.builder("fb.risk.SymbolService")
                .addMethod(method, ServerCalls.asyncServerStreamingCall(this::getSymbols))
                .build();

        try {
            return Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                    .executor(Executors.newVirtualThreadPerTaskExecutor())
                    .addService(service)
                    .build()
                    .start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void getSymbols(List<String> symbols, StreamObserver<ByteBuffer> responseObserver) {
        for (String sym : symbols) {
            Optional<ByteBuffer> buf = reader.sliceFor(sym);
            buf.ifPresent(responseObserver::onNext);
        }
        responseObserver.onCompleted();
    }
}

