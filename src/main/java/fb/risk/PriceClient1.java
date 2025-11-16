package fb.risk;

import io.grpc.*;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class PriceClient1 {
    public static void main(String[] args) {
        ManagedChannel ch = NettyChannelBuilder
                .forAddress("localhost", 9000)
                .usePlaintext()
                .build();

        ClientCall<ByteBuf, ByteBuf> call = ch.newCall(PriceServer.GET_PRICES, CallOptions.DEFAULT);

        call.start(new ClientCall.Listener<>() {
            @Override
            public void onMessage(ByteBuf msg) {
                System.out.println("Received slice size = " + msg.readableBytes());
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                System.out.println("DONE: " + status);
            }
        }, new Metadata());

        ByteBuf req = Unpooled.wrappedBuffer("AAPL".getBytes());
        call.sendMessage(req);
        call.halfClose();
        call.request(10);
    }
}

