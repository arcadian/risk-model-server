package fb.risk;

import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.file.Path;

public class PriceServer {

    static final MethodDescriptor<ByteBuf, ByteBuf> GET_PRICES =
        MethodDescriptor.<ByteBuf, ByteBuf>newBuilder()
            .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
            .setFullMethodName("PriceService/GetPrices")
            .setRequestMarshaller(new ByteBufMarshaller())
            .setResponseMarshaller(new ByteBufMarshaller())
            .build();

    public static void main(String[] args) throws Exception {
        FlatDBReader reader = new FlatDBReader(Path.of("symbols.flatdb"));

        ServerServiceDefinition svc = ServerServiceDefinition.builder("PriceService")
            .addMethod(GET_PRICES, new ServerCalls.ServerStreamingMethod<>() {
                @Override
                public void invoke(ByteBuf req, StreamObserver<ByteBuf> resp) {
                    String sym = req.toString(io.netty.util.CharsetUtil.UTF_8);
                    reader.sliceFor(sym).ifPresentOrElse(slice -> {
                        ByteBuf out = Unpooled.wrappedBuffer(slice);
                        resp.onNext(out);         // zero-copy
                        resp.onCompleted();
                    }, () -> {
                        resp.onCompleted();        // no symbol
                    });
                }
            }).build();

        Server server = NettyServerBuilder
                .forPort(9000)
                .addService(svc)
                .build()
                .start();

        System.out.println("Server started @ 9000");
        server.awaitTermination();
    }
}

