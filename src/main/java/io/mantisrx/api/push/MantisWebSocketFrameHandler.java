package io.mantisrx.api.push;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;

import com.netflix.config.DynamicIntProperty;
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
import rx.Observable;
import rx.Subscription;

@Slf4j
public class MantisWebSocketFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {
    private final ConnectionBroker connectionBroker;
    private Subscription subscription;
    private final DynamicIntProperty queueCapacity = new DynamicIntProperty("io.mantisrx.api.push.queueCapacity", 1000);
    private final DynamicIntProperty writeIntervalMillis = new DynamicIntProperty("io.mantisrx.api.push.writeIntervalMillis", 50);

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

            BlockingQueue<String> queue = new LinkedBlockingQueue<String>(queueCapacity.get());

            this.subscription = this.connectionBroker.connect(pcd)
                    .mergeWith(Observable.interval(writeIntervalMillis.get(), TimeUnit.MILLISECONDS)
                                 .map(__ -> Constants.DUMMY_TIMER_DATA))
                    .doOnNext(event -> {
                        if (!Constants.DUMMY_TIMER_DATA.equals(event) && !queue.offer(event)) {
                            numDroppedBytesCounter.increment(event.length());
                            numDroppedMessagesCounter.increment();
                        }
                    })
                    .filter(Constants.DUMMY_TIMER_DATA::equals)
                    .doOnNext(__ -> {
                        if (ctx.channel().isWritable()) {
                            final List<String> items = new ArrayList<>(queue.size());
                            queue.drainTo(items);
                            for (String event : items) {
                                ctx.writeAndFlush(new TextWebSocketFrame(event));
                                numMessagesCounter.increment();
                                numBytesCounter.increment(event.length());
                            }
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
