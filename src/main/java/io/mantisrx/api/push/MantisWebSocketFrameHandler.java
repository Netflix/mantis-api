package io.mantisrx.api.push;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;

import com.netflix.config.DynamicIntProperty;
import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.Constants;
import io.mantisrx.api.Util;
import io.mantisrx.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
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
    private final DynamicIntProperty queueCapacity = new DynamicIntProperty("io.mantisrx.api.push.queueCapacity", 1000);
    private final DynamicIntProperty writeIntervalMillis = new DynamicIntProperty("io.mantisrx.api.push.writeIntervalMillis", 50);

    private Subscription subscription;
    private String uri;
    private ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().setNameFormat("websocket-handler-drainer-%d").build());
    private ScheduledFuture drainFuture;

    public MantisWebSocketFrameHandler(ConnectionBroker broker) {
        super(true);
        this.connectionBroker = broker;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt.getClass() == WebSocketServerProtocolHandler.HandshakeComplete.class) {
            WebSocketServerProtocolHandler.HandshakeComplete complete = (WebSocketServerProtocolHandler.HandshakeComplete) evt;

            uri = complete.requestUri();
            final PushConnectionDetails pcd = PushConnectionDetails.from(uri);

            log.info("Request to URI '{}' is a WebSSocket upgrade, removing the SSE handler", uri);
            if (ctx.pipeline().get(MantisSSEHandler.class) != null) {
                ctx.pipeline().remove(MantisSSEHandler.class);
            }

            final String[] tags = Util.getTaglist(uri, pcd.target);
            Counter numDroppedBytesCounter = SpectatorUtils.newCounter(Constants.numDroppedBytesCounterName, pcd.target, tags);
            Counter numDroppedMessagesCounter = SpectatorUtils.newCounter(Constants.numDroppedMessagesCounterName, pcd.target, tags);
            Counter numMessagesCounter = SpectatorUtils.newCounter(Constants.numMessagesCounterName, pcd.target, tags);
            Counter numBytesCounter = SpectatorUtils.newCounter(Constants.numBytesCounterName, pcd.target, tags);
            Counter drainTriggeredCounter = SpectatorUtils.newCounter(Constants.drainTriggeredCounterName, pcd.target, tags);
            Counter numIncomingMessagesCounter = SpectatorUtils.newCounter(Constants.numIncomingMessagesCounterName, pcd.target, tags);

            BlockingQueue<String> queue = new LinkedBlockingQueue<>(queueCapacity.get());

            drainFuture = scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    if (queue.size() > 0 && ctx.channel().isWritable()) {
                        drainTriggeredCounter.increment();
                        final List<String> items = new ArrayList<>(queue.size());
                        synchronized (queue) {
                            queue.drainTo(items);
                        }
                        for (String data : items) {
                            ctx.write(new TextWebSocketFrame(data));
                            numMessagesCounter.increment();
                            numBytesCounter.increment(data.length());
                        }
                        ctx.flush();
                    }
                } catch (Exception ex) {
                    log.error("Error writing to channel", ex);
                }
            }, writeIntervalMillis.get(), writeIntervalMillis.get(), TimeUnit.MILLISECONDS);

            this.subscription = this.connectionBroker.connect(pcd)
                    .doOnNext(event -> {
                        numIncomingMessagesCounter.increment();
                        if (!Constants.DUMMY_TIMER_DATA.equals(event)) {
                            boolean offer = false;
                            synchronized (queue) {
                                offer = queue.offer(event);
                            }
                            if (!offer) {
                                numDroppedBytesCounter.increment(event.length());
                                numDroppedMessagesCounter.increment();
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
        log.info("Channel {} is unregistered. URI: {}", ctx.channel(), uri);
        unsubscribeIfSubscribed();
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel {} is inactive. URI: {}", ctx.channel(), uri);
        unsubscribeIfSubscribed();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Exception caught by channel {}. URI: {}", ctx.channel(), uri, cause);
        unsubscribeIfSubscribed();
        // This is the tail of handlers. We should close the channel between the server and the client,
        // essentially causing the client to disconnect and terminate.
        ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) {
        // No op.
    }

    /** Unsubscribe if it's subscribed. */
    private void unsubscribeIfSubscribed() {
        if (subscription != null && !subscription.isUnsubscribed()) {
            log.info("WebSocket unsubscribing subscription with URI: {}", uri);
            subscription.unsubscribe();
        }
        if (drainFuture != null) {
            drainFuture.cancel(false);
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
        }
    }
}
