package io.mantisrx.api.push;

import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.Constants;
import io.mantisrx.api.Util;
import io.mantisrx.server.core.master.MasterDescription;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.channel.StringTransformer;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.client.HttpResponseHeaders;
import rx.Observable;
import rx.Subscription;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Http handler for the WebSocket/SSE paths.
 */
@Slf4j
public class MantisSSEHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final ConnectionBroker connectionBroker;
    private final MasterClientWrapper masterClientWrapper;
    private final List<String> pushPrefixes;
    private Subscription subscription;

    public MantisSSEHandler(ConnectionBroker connectionBroker, MasterClientWrapper masterClientWrapper,
                            List<String> pushPrefixes) {
        this.connectionBroker = connectionBroker;
        this.masterClientWrapper = masterClientWrapper;
        this.pushPrefixes = pushPrefixes;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        if (Util.startsWithAnyOf(request.uri(), pushPrefixes)
                && !isWebsocketUpgrade(request)) {

            if (HttpUtil.is100ContinueExpected(request)) {
                send100Contine(ctx);
            }



            HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK);
            HttpHeaders headers = response.headers();
            headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
            headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Origin, X-Requested-With, Accept, Content-Type, Cache-Control");
            headers.set(HttpHeaderNames.CONTENT_TYPE, "text/event-stream");
            headers.set(HttpHeaderNames.CACHE_CONTROL, "no-cache, no-store, max-age=0, must-revalidate");
            headers.set(HttpHeaderNames.PRAGMA, HttpHeaderValues.NO_CACHE);
            headers.set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(response);

            final String uri = request.uri();
            final PushConnectionDetails pcd =
                    isSubmitAndConnect(request)
                            ? new PushConnectionDetails(uri, jobSubmit(request), PushConnectionDetails.TARGET_TYPE.CONNECT_BY_ID)
                            : PushConnectionDetails.from(uri);
            log.info("SSE Connecting for: {}", pcd);

            boolean tunnelPingsEnabled = isTunnelPingsEnabled(uri);

            final String[] tags = Util.getTaglist(uri, "NONE"); // TODO: Attach an ID to streaming calls via Zuul. This will help access log too.
            Counter numDroppedBytesCounter = SpectatorUtils.newCounter(Constants.numDroppedBytesCounterName, "NONE", tags);
            Counter numDroppedMessagesCounter = SpectatorUtils.newCounter(Constants.numDroppedMessagesCounterName, "NONE", tags);
            Counter numMessagesCounter = SpectatorUtils.newCounter(Constants.numMessagesCounterName, "NONE", tags);
            Counter numBytesCounter = SpectatorUtils.newCounter(Constants.numBytesCounterName, "NONE", tags);

            this.subscription = this.connectionBroker.connect(pcd)
                    .mergeWith(tunnelPingsEnabled
                            ? Observable.interval(Constants.TunnelPingIntervalSecs, Constants.TunnelPingIntervalSecs,
                            TimeUnit.SECONDS)
                            .map(l -> Constants.TunnelPingMessage)
                            : Observable.empty())
                    .doOnNext(event -> {
                        String data = Constants.SSE_DATA_PREFIX + event + Constants.SSE_DATA_SUFFIX;
                        if (ctx.channel().isWritable()) {
                            ctx.writeAndFlush(Unpooled.copiedBuffer(data, StandardCharsets.UTF_8));
                            numMessagesCounter.increment();
                            numBytesCounter.increment(data.length());
                        } else {
                            numDroppedBytesCounter.increment(data.length());
                            numDroppedMessagesCounter.increment();
                        }
                    })
                    .subscribe();

        } else {
            ctx.fireChannelRead(request.retain());
        }
    }

    private static void send100Contine(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.CONTINUE);
        ctx.writeAndFlush(response);
    }

    private boolean isTunnelPingsEnabled(String uri) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
        return queryStringDecoder.parameters()
                .getOrDefault(Constants.TunnelPingParamName, Arrays.asList("false"))
                .get(0)
                .equalsIgnoreCase("true");
    }

    private boolean isWebsocketUpgrade(HttpRequest request) {
        HttpHeaders headers = request.headers();
        return "Upgrade".equalsIgnoreCase(headers.get(HttpHeaderNames.CONNECTION)) &&
                "WebSocket".equalsIgnoreCase(headers.get(HttpHeaderNames.UPGRADE));
    }


    private boolean isSubmitAndConnect(HttpRequest request) {
        return request.method().equals(HttpMethod.POST) && request.uri().contains("jobsubmitandconnect");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        if (this.subscription != null && !this.subscription.isUnsubscribed()) {
            this.subscription.unsubscribe();
        }
        super.channelUnregistered(ctx);
    }

    public String jobSubmit(FullHttpRequest request) {
        final String API_JOB_SUBMIT_PATH = "/api/submit";

        String content = request.content().toString(StandardCharsets.UTF_8);
        return callPostOnMaster(masterClientWrapper.getMasterMonitor().getMasterObservable(), API_JOB_SUBMIT_PATH, content)
                .retryWhen(Util.getRetryFunc(log, API_JOB_SUBMIT_PATH))
                .flatMap(masterResponse -> masterResponse.getByteBuf()
                        .take(1)
                        .map(byteBuf -> {
                            final String s = byteBuf.toString(StandardCharsets.UTF_8);
                            log.info("response: " + s);
                            return s;
                        }))
                .take(1)
                .toBlocking()
                .first();
    }

    public static class MasterResponse {

        private final HttpResponseStatus status;
        private final Observable<ByteBuf> byteBuf;
        private final HttpResponseHeaders responseHeaders;

        public MasterResponse(HttpResponseStatus status, Observable<ByteBuf> byteBuf, HttpResponseHeaders responseHeaders) {
            this.status = status;
            this.byteBuf = byteBuf;
            this.responseHeaders = responseHeaders;
        }

        public HttpResponseStatus getStatus() {
            return status;
        }

        public Observable<ByteBuf> getByteBuf() {
            return byteBuf;
        }

        public HttpResponseHeaders getResponseHeaders() { return responseHeaders; }
    }

    public static Observable<MasterResponse> callPostOnMaster(Observable<MasterDescription> masterObservable, String uri, String content) {
        PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<String>> pipelineConfigurator
                = PipelineConfigurators.httpClientConfigurator();

        return masterObservable
                .filter(Objects::nonNull)
                .flatMap(masterDesc -> {
                    HttpClient<String, ByteBuf> client =
                            RxNetty.<String, ByteBuf>newHttpClientBuilder(masterDesc.getHostname(), masterDesc.getApiPort())
                                    .pipelineConfigurator(pipelineConfigurator)
                                    .build();
                    HttpClientRequest<String> request = HttpClientRequest.create(HttpMethod.POST, uri);
                    request = request.withHeader(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_JSON.toString());
                    request.withRawContent(content, StringTransformer.DEFAULT_INSTANCE);
                    return client.submit(request)
                            .map(response -> new MasterResponse(response.getStatus(), response.getContent(), response.getHeaders()));
                })
                .take(1);
    }
}
