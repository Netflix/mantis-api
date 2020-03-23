/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.api.tunnel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.util.Constants;
import io.mantisrx.api.util.RetryUtils;
import io.mantisrx.api.util.Util;
import io.mantisrx.common.MantisServerSentEvent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.channel.StringTransformer;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.mantisrx.api.util.Constants.*;
import static io.mantisrx.api.util.Util.getLocalRegion;

@Slf4j
public class CrossRegionHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    public static final String metaErrorMsgHeader = "mantis.meta.error.message";
    public static final String metaOriginName = "mantis.meta.origin";

    private static final String numRemoteBytesCounterName = "numRemoteSinkBytes";
    private static final String numRemoteMessagesCounterName = "numRemoteMessages";
    private static final String numSseErrorsCounterName = "numSseErrors";
    public static final long TunnelPingIntervalSecs = 12;


    private final List<String> pushPrefixes;
    private final MantisCrossRegionalClient mantisCrossRegionalClient;
    private final Scheduler scheduler;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Subscription subscription = null;

    public CrossRegionHandler(
            List<String> pushPrefixes,
            MantisCrossRegionalClient mantisCrossRegionalClient,
            Scheduler scheduler) {
        this.pushPrefixes = pushPrefixes;
        this.mantisCrossRegionalClient = mantisCrossRegionalClient;
        this.scheduler = scheduler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {

        // What cases do we need to handle?
        if (HttpUtil.is100ContinueExpected(request)) {
            send100Contine(ctx);
        }

        if (isCrossRegionStreamingPath(request.uri())) {
            handleRemoteSse(ctx, request);
        } else { // REST
            if (request.method() == HttpMethod.HEAD) {
                handleHead(ctx, request);
            } else if (request.method() == HttpMethod.GET) {
                handleRestGet(ctx, request);
            } else if(request.method() == HttpMethod.POST) {
                handleRestPost(ctx, request);
            }
        }
    }

    //
    // REST Implementations
    //

    private void handleHead(ChannelHandlerContext ctx, FullHttpRequest request) {

        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS,
                "Origin, X-Requested-With, Accept, Content-Type, Cache-Control");
        headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS,
                "GET, OPTIONS, PUT, POST, DELETE, CONNECT");

        HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.copiedBuffer("", Charset.defaultCharset()),
                headers,
                new DefaultHttpHeaders());
        ctx.writeAndFlush(response)
                .addListener(__ -> ctx.close());
    }

    private void handleRestGet(ChannelHandlerContext ctx, FullHttpRequest request) {
        List<String> regions = getRegion(request.uri()).equals("all")
                ? Arrays.asList("us-east-1", "us-west-2", "eu-west-1")
                : Collections.singletonList(getRegion(request.uri()));

        String uri = getTail(request.uri());
        Observable.from(regions)
                .flatMap(region -> {
                    final AtomicReference<Throwable> ref = new AtomicReference<>();
                    HttpClientRequest<String> rq = HttpClientRequest.create(HttpMethod.GET, uri);

                    return Observable
                            .create((Observable.OnSubscribe<HttpClient<String, ByteBuf>>) subscriber ->
                                    subscriber.onNext(mantisCrossRegionalClient.getSecureRestClient(region)))
                            .flatMap(client -> {
                                ref.set(null);
                                return client.submit(rq)
                                        .flatMap(resp -> {
                                            final int code = resp.getStatus().code();
                                            if (code >= 500) {
                                                throw new RuntimeException(resp.getStatus().toString());
                                            }
                                            return responseToRegionData(region, resp);
                                        })
                                        .onErrorReturn(t -> {
                                            log.warn("Error getting response from remote master: " + t.getMessage());
                                            ref.set(t);
                                            return new RegionData(region, false, t.getMessage(), 0);
                                        });
                            })
                            .map(data -> {
                                final Throwable t = ref.get();
                                if (t != null)
                                    throw new RuntimeException(t);
                                return data;
                            })
                            .retryWhen(RetryUtils.getRetryFunc(log, uri + " in " + region))
                            .take(1)
                            .onErrorReturn(t -> new RegionData(region, false, t.getMessage(), 0));
                })
                .reduce(new ArrayList<RegionData>(3), (regionDatas, regionData) -> {
                    regionDatas.add(regionData);
                    return regionDatas;
                })
                .observeOn(scheduler)
                .subscribeOn(scheduler)
                .take(1)
                .subscribe(result -> writeDataAndCloseChannel(ctx, result));
    }

    private void handleRestPost(ChannelHandlerContext ctx, FullHttpRequest request) {
        String uri = getTail(request.uri());
        List<String> regions = getRegion(request.uri()).equals("all")
                ? Arrays.asList("us-east-1", "us-west-2", "eu-west-1")
                : Collections.singletonList(getRegion(request.uri()));

        log.info("Relaying POST URI {} to {}.", uri, regions);
        final AtomicReference<Throwable> ref = new AtomicReference<>();

        String content = request.content().toString(Charset.defaultCharset());
        Observable.from(regions)
                .flatMap(region -> {
                    HttpClientRequest<String> rq = HttpClientRequest.create(HttpMethod.POST, uri);
                    rq.withRawContent(content, StringTransformer.DEFAULT_INSTANCE);

                    return Observable
                            .create((Observable.OnSubscribe<HttpClient<String, ByteBuf>>) subscriber ->
                                    subscriber.onNext(mantisCrossRegionalClient.getSecureRestClient(region)))
                            .flatMap(client -> client.submit(rq)
                                    .flatMap(resp -> {
                                        final int code = resp.getStatus().code();
                                        if (code >= 500) {
                                            throw new RuntimeException(resp.getStatus().toString() + "in " + region );
                                        }
                                        return responseToRegionData(region, resp);
                                    })
                                    .onErrorReturn(t -> {
                                        log.warn("Error getting response from remote master: " + t.getMessage());
                                        ref.set(t);
                                        return new RegionData(region, false, t.getMessage(), 0);
                                    }))
                            .map(data -> {
                                final Throwable t = ref.get();
                                if (t != null)
                                    throw new RuntimeException(t);
                                return data;
                            })
                            .retryWhen(RetryUtils.getRetryFunc(log, uri + " in " + region))
                            .take(1)
                            .onErrorReturn(t -> new RegionData(region, false, t.getMessage(), 0));
                })
                .reduce(new ArrayList<RegionData>(), (regionDatas, regionData) -> {
                    regionDatas.add(regionData);
                    return regionDatas;
                })
                .observeOn(scheduler)
                .subscribeOn(scheduler)
                .take(1)
                .subscribe(result -> writeDataAndCloseChannel(ctx, result));
    }

    private void handleRemoteSse(ChannelHandlerContext ctx, FullHttpRequest request) {
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

        final boolean sendThroughTunnelPings = hasTunnelPingParam(request.uri());
        final String uri = uriWithTunnelParamsAdded(getTail(request.uri()));
        List<String> regions = getRegion(request.uri()).equals("all")
                ? Arrays.asList("us-east-1", "us-west-2", "eu-west-1")
                : Collections.singletonList(getRegion(request.uri()));

        log.info("Initiating remote SSE connection to {} in {}.", uri, regions);
        final String[] tags = Util.getTaglist(request.uri(), "NONE"); // TODO: Attach an ID to streaming calls via Zuul. This will help access log too.
        Counter numDroppedBytesCounter = SpectatorUtils.newCounter(Constants.numDroppedBytesCounterName, "NONE", tags);
        Counter numDroppedMessagesCounter = SpectatorUtils.newCounter(Constants.numDroppedMessagesCounterName, "NONE", tags);
        Counter numMessagesCounter = SpectatorUtils.newCounter(Constants.numMessagesCounterName, "NONE", tags);
        Counter numBytesCounter = SpectatorUtils.newCounter(Constants.numBytesCounterName, "NONE", tags);

        // TODO: Get the connection broker to share these.
        subscription = Observable.from(regions)
                .flatMap(region -> {
                    log.info("Connecting to remote region {} at {}.", region, uri);
                    return mantisCrossRegionalClient.getSecureSseClient(region)
                            .submit(HttpClientRequest.createGet(uri))
                            .retryWhen(RetryUtils.getRetryFunc(log, uri + " in " + region))
                            .doOnError(throwable -> log.warn(
                                    "Error getting response from remote SSE server for uri {} in region {}: {}",
                                    uri, region, throwable.getMessage(), throwable)
                            ).flatMap(remoteResponse -> {
                                if (!remoteResponse.getStatus().reasonPhrase().equals("OK")) {
                                    log.warn("Unexpected response from remote sink for uri {} region {}: {}", uri, region, remoteResponse.getStatus().reasonPhrase());
                                    String err = remoteResponse.getHeaders().get(metaErrorMsgHeader);
                                    if (err == null || err.isEmpty())
                                        err = remoteResponse.getStatus().reasonPhrase();
                                    return Observable.<MantisServerSentEvent>error(new Exception(err));
                                }
                                final String originReplacement = "\\{\"" + metaOriginName + "\": \"" + region + "\", ";
                                return streamContent(remoteResponse, region, uri, sendThroughTunnelPings)
                                        .map(datum -> datum.getEventAsString().replaceFirst("^\\{", originReplacement))
                                        .doOnError(t -> log.error(t.getMessage()));
                            })
                            .subscribeOn(scheduler)
                            .observeOn(scheduler)
                            .doOnError(t -> log.warn("Error streaming in remote data ({}). Will retry: {}", region, t.getMessage(), t))
                            .doOnCompleted(() -> log.info(String.format("remote sink connection complete for uri %s, region=%s", uri, region)));
                })
                .observeOn(scheduler)
                .subscribeOn(scheduler)
                .doOnError(t -> log.error("Error in flatMapped cross-regional observable for {}", uri, t))
                .subscribe(result -> {
                    String data = SSE_DATA_PREFIX + result + SSE_DATA_SUFFIX;
                    if (ctx.channel().isWritable()) {
                        ctx.writeAndFlush(Unpooled.copiedBuffer(data, StandardCharsets.UTF_8));
                        numMessagesCounter.increment();
                        numBytesCounter.increment(data.length());
                    } else {
                        numDroppedBytesCounter.increment(data.length());
                        numDroppedMessagesCounter.increment();
                    }
                });
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        if (subscription != null && !subscription.isUnsubscribed()) {
            subscription.unsubscribe();
        }
    }

    private boolean hasTunnelPingParam(String uri) {
        return uri != null && uri.contains(TunnelPingParamName);
    }

    private Observable<MantisServerSentEvent> streamContent(HttpClientResponse<ServerSentEvent> response, String
            region, String uri, boolean passThruTunnelPings) {

        Counter numRemoteBytes = SpectatorUtils.newCounter(numRemoteBytesCounterName, "", "region", region);
        Counter numRemoteMessages = SpectatorUtils.newCounter(numRemoteMessagesCounterName, "", "region", region);
        Counter numSseErrors = SpectatorUtils.newCounter(numSseErrorsCounterName, "", "region", region);

        return response.getContent()
                .doOnError(t -> log.warn(t.getMessage()))
                .timeout(3 * TunnelPingIntervalSecs, TimeUnit.SECONDS)
                .doOnError(t -> log.warn("Timeout getting data from remote {} connection for {}", region, uri))
                .filter(sse -> !(!sse.hasEventType() || !sse.getEventTypeAsString().startsWith("error:")) ||
                        passThruTunnelPings || !TunnelPingMessage.equals(sse.contentAsString()))
                .map(t1 -> {
                    String data = "";
                    if (t1.hasEventType() && t1.getEventTypeAsString().startsWith("error:")) {
                        log.error("SSE has error, type=" + t1.getEventTypeAsString() + ", content=" + t1.contentAsString());
                        numSseErrors.increment();
                        throw new RuntimeException("Got error SSE event: " + t1.contentAsString());
                    }
                    try {
                        data = t1.contentAsString();
                        if (data != null) {
                            numRemoteBytes.increment(data.length());
                            numRemoteMessages.increment();
                        }
                    } catch (Exception e) {
                        log.error("Could not extract data from SSE " + e.getMessage(), e);
                    }
                    return new MantisServerSentEvent(data);
                });
    }

    private Observable<RegionData> responseToRegionData(String region, HttpClientResponse<ByteBuf> resp) {
        final int code = resp.getStatus().code();
        return resp.getContent()
                .collect(Unpooled::buffer,
                        ByteBuf::writeBytes)
                .map(byteBuf -> new RegionData(region, true,
                        byteBuf.toString(StandardCharsets.UTF_8), code)
                )
                .onErrorReturn(t -> new RegionData(region, false, t.getMessage(), code));
    }

    private void writeDataAndCloseChannel(ChannelHandlerContext ctx, ArrayList<RegionData> result) {
        try {
            String serialized = responseToString(result);
            HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(serialized, Charset.defaultCharset()));

            HttpHeaders headers = response.headers();
            headers.add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON + "; charset=utf-8");
            headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
            headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS,
                    "Origin, X-Requested-With, Accept, Content-Type, Cache-Control");
            headers.add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS,
                    "GET, OPTIONS, PUT, POST, DELETE, CONNECT");
            headers.add(HttpHeaderNames.CONTENT_LENGTH, serialized.length());

            ctx.writeAndFlush(response)
                    .addListener(__ -> ctx.close());
        } catch (Exception ex) {
            log.error("Error serializing cross regional response: {}", ex.getMessage(), ex);
        }
    }

    private String uriWithTunnelParamsAdded(String uri) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
        QueryStringEncoder queryStringEncoder = new QueryStringEncoder(queryStringDecoder.path());

        queryStringDecoder.parameters().forEach((key, value) -> value.forEach(val -> queryStringEncoder.addParam(key, val)));

        queryStringEncoder.addParam(TunnelPingParamName, "true");
        queryStringEncoder.addParam(TagsParamName, OriginRegionTagName + TagNameValDelimiter + getLocalRegion());
        return queryStringEncoder.toString();
    }

    // TODO: Duplicate with MantisSSEHandler
    private static void send100Contine(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.CONTINUE);
        ctx.writeAndFlush(response);
    }

    private boolean isCrossRegionStreamingPath(String uri) {
        return Util.startsWithAnyOf(getTail(uri), this.pushPrefixes);
    }

    private static String getTail(String uri) {
        return uri.replaceFirst("^/region/.*?/", "/");
    }

    private static String getRegion(String uri) {
        return uri.replaceFirst("^/region/", "")
                .replaceFirst("/.*$", "");
    }

    private static String responseToString(List<RegionData> dataList) {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (RegionData data : dataList) {
            if (first)
                first = false;
            else {
                sb.append(",");
            }
            if (data.isSuccess()) {
                String outputData = getForceWrappedJson(data.getData(), data.getRegion(), data.getResponseCode(), null);
                sb.append(outputData);
            } else {
                sb.append(getForceWrappedJson("", data.getRegion(), data.getResponseCode(), data.getData()));
            }
        }
        sb.append("]");
        return sb.toString();
    }

    private final static String regKey = "mantis.meta.origin";
    private final static String errKey = "mantis.meta.errorString";
    private final static String codeKey = "mantis.meta.origin.response.code";

    public static String getWrappedJson(String data, String region, String err) {
        return getWrappedJsonIntl(data, region, err, 0, false);
    }

    public static String getForceWrappedJson(String data, String region, int code, String err) {
        return getWrappedJsonIntl(data, region, err, code, true);
    }

    private static String getWrappedJsonIntl(String data, String region, String err, int code, boolean forceJson) {
        try {
            JSONObject o = new JSONObject(data);
            o.put(regKey, region);
            if (err != null && !err.isEmpty())
                o.put(errKey, err);
            if (code > 0)
                o.put(codeKey, "" + code);
            return o.toString();
        } catch (JSONException e) {
            try {
                JSONArray a = new JSONArray(data);
                if (!forceJson)
                    return data;
                JSONObject o = new JSONObject();
                o.put(regKey, region);
                if (err != null && !err.isEmpty())
                    o.put(errKey, err);
                if (code > 0)
                    o.put(codeKey, "" + code);
                o.accumulate("response", a);
                return o.toString();
            } catch (JSONException ae) {
                if (!forceJson)
                    return data;
                JSONObject o = new JSONObject();
                o.put(regKey, region);
                if (err != null && !err.isEmpty())
                    o.put(errKey, err);
                if (code > 0)
                    o.put(codeKey, "" + code);
                o.put("response", data);
                return o.toString();
            }
        }
    }
}

