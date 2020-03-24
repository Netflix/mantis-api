package io.mantisrx.api.push;

import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.Constants;
import io.mantisrx.api.Util;
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
            final PushConnectionDetails pcd = PushConnectionDetails.from(uri);

            final String[] tags = Util.getTaglist(uri, "NONE"); // TODO: Attach an ID to streaming calls via Zuul. This will help access log too.
            Counter numDroppedBytesCounter = SpectatorUtils.newCounter(Constants.numDroppedBytesCounterName, "NONE", tags);
            Counter numDroppedMessagesCounter = SpectatorUtils.newCounter(Constants.numDroppedMessagesCounterName, "NONE", tags);
            Counter numMessagesCounter = SpectatorUtils.newCounter(Constants.numMessagesCounterName, "NONE", tags);
            Counter numBytesCounter = SpectatorUtils.newCounter(Constants.numBytesCounterName, "NONE", tags);

            this.subscription = this.connectionBroker.connect(pcd)
                    .doOnNext(event -> {
                        if (ctx.channel().isWritable()) {
                            ctx.writeAndFlush(new TextWebSocketFrame(event));
                            numMessagesCounter.increment();
                            numBytesCounter.increment(event.length());
                        } else {
                            numDroppedBytesCounter.increment(event.length());
                            numDroppedMessagesCounter.increment();
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
