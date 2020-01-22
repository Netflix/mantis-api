package io.mantisrx.api.push;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import lombok.extern.slf4j.Slf4j;
import rx.Subscription;

@Slf4j
public class MantisWebSocketFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {
    private final ConnectionBroker connectionBroker;
    private Subscription subscription;

    public MantisWebSocketFrameHandler(ConnectionBroker broker) {
        this.connectionBroker = broker;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt.getClass() == WebSocketServerProtocolHandler.HandshakeComplete.class) {

            if (ctx.pipeline().get(MantisSSEHandler.class) != null) {
                ctx.pipeline().remove(MantisSSEHandler.class);
            }

            WebSocketServerProtocolHandler.HandshakeComplete complete = (WebSocketServerProtocolHandler.HandshakeComplete) evt;

            final String uri = complete.requestUri();
            final PushConnectionDetails pcd = new PushConnectionDetails(PushConnectionDetails.determineTarget(uri),
                    PushConnectionDetails.determineTargetType(uri));

            this.subscription = this.connectionBroker.connect(pcd)
                    .doOnNext(event -> {
                        if (ctx.channel().isWritable()) {
                            ctx.writeAndFlush(new TextWebSocketFrame(event));
                        }
                    })
            .subscribe();

        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        if (subscription != null && !subscription.isUnsubscribed()) {
            this.subscription.unsubscribe();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) throws Exception {
        // No op.
    }


}
