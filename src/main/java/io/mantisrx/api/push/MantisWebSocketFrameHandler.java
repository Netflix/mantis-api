package io.mantisrx.api.push;

import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.Constants;
import io.mantisrx.api.Util;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import rx.Subscription;

@Slf4j
public class MantisWebSocketFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {
    private final ConnectionBroker connectionBroker;
    private Subscription subscription;

    public MantisWebSocketFrameHandler(ConnectionBroker broker) {
        super(true);
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

            final String[] tags = Util.getTaglist(uri, pcd.target);
            Counter numDroppedBytesCounter = SpectatorUtils.newCounter(Constants.numDroppedBytesCounterName, pcd.target, tags);
            Counter numDroppedMessagesCounter = SpectatorUtils.newCounter(Constants.numDroppedMessagesCounterName, pcd.target, tags);
            Counter numMessagesCounter = SpectatorUtils.newCounter(Constants.numMessagesCounterName, pcd.target, tags);
            Counter numBytesCounter = SpectatorUtils.newCounter(Constants.numBytesCounterName, pcd.target, tags);

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
            ReferenceCountUtil.retain(evt);
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
