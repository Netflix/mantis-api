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

import com.google.common.annotations.VisibleForTesting;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicStringProperty;
import com.netflix.spectator.api.Counter;
import com.netflix.zuul.netty.SpectatorUtils;
import io.mantisrx.api.Constants;
import io.mantisrx.api.Util;
import io.mantisrx.api.push.ConnectionBroker;
import io.mantisrx.api.push.PushConnectionDetails;
import io.mantisrx.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.QueryStringEncoder;
import lombok.extern.slf4j.Slf4j;
import mantis.io.reactivex.netty.channel.StringTransformer;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.mantisrx.api.Constants.OriginRegionTagName;
import static io.mantisrx.api.Constants.TagNameValDelimiter;
import static io.mantisrx.api.Constants.TagsParamName;
import static io.mantisrx.api.Constants.TunnelPingMessage;
import static io.mantisrx.api.Constants.TunnelPingParamName;
import static io.mantisrx.api.Util.getLocalRegion;

@Slf4j
public class CrossRegionHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final List<String> pushPrefixes;
    private final MantisCrossRegionalClient mantisCrossRegionalClient;
    private final ConnectionBroker connectionBroker;
    private final Scheduler scheduler;

    private Subscription subscription = null;
    private String uriForLogging = null;
    private ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().setNameFormat("cross-region-handler-drainer-%d").build());
    private ScheduledFuture drainFuture;
    private final DynamicIntProperty queueCapacity = new DynamicIntProperty("io.mantisrx.api.push.queueCapacity", 1000);
    private final DynamicIntProperty writeIntervalMillis = new DynamicIntProperty("io.mantisrx.api.push.writeIntervalMillis", 50);
    private final DynamicStringProperty tunnelRegionsProperty = new DynamicStringProperty("io.mantisrx.api.tunnel.regions", Util.getLocalRegion());

    public CrossRegionHandler(
            List<String> pushPrefixes,
            MantisCrossRegionalClient mantisCrossRegionalClient,
            ConnectionBroker connectionBroker,
            Scheduler scheduler) {
        super(true);
        this.pushPrefixes = pushPrefixes;
        this.mantisCrossRegionalClient = mantisCrossRegionalClient;
        this.connectionBroker = connectionBroker;
        this.scheduler = scheduler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        uriForLogging = request.uri();
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
            } else {
                ctx.fireChannelRead(request.retain());
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

    @VisibleForTesting
    List<String> getTunnelRegions() {
        return parseRegionCsv(tunnelRegionsProperty.get());
    }

    private static List<String> parseRegionCsv(String regionCsv) {
        return Arrays.stream(regionCsv.split(","))
                .map(String::trim)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    List<String> parseRegionsInUri(String uri) {
        final String regionString = getRegion(uri);
        if (isAllRegion(regionString)) {
            return getTunnelRegions();
        } else if (regionString.contains(",")) {
            return parseRegionCsv(regionString);
        } else {
            return Collections.singletonList(regionString);
        }
    }

    private void handleRestGet(ChannelHandlerContext ctx, FullHttpRequest request) {
        List<String> regions = parseRegionsInUri(request.uri());

        String uri = getTail(request.uri());
        log.info("Relaying GET URI {} to {} (original uri {}).", uri, regions, request.uri());
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
                            .retryWhen(Util.getRetryFunc(log, uri + " in " + region))
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
        List<String> regions = parseRegionsInUri(request.uri());

        log.info("Relaying POST URI {} to {} (original uri {}).", uri, regions, request.uri());
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
                            .retryWhen(Util.getRetryFunc(log, uri + " in " + region))
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
        List<String> regions = parseRegionsInUri(request.uri());

        log.info("Initiating remote SSE connection to {} in {} (original URI: {}).", uri, regions, request.uri());
        PushConnectionDetails pcd = PushConnectionDetails.from(uri, regions);

        String[] tags = Util.getTaglist(request.uri(), pcd.target, getRegion(request.uri()));

        Counter numDroppedBytesCounter = SpectatorUtils.newCounter(Constants.numDroppedBytesCounterName, pcd.target, tags);
        Counter numDroppedMessagesCounter = SpectatorUtils.newCounter(Constants.numDroppedMessagesCounterName, pcd.target, tags);
        Counter numMessagesCounter = SpectatorUtils.newCounter(Constants.numMessagesCounterName, pcd.target, tags);
        Counter numBytesCounter = SpectatorUtils.newCounter(Constants.numBytesCounterName, pcd.target, tags);
        Counter drainTriggeredCounter = SpectatorUtils.newCounter(Constants.drainTriggeredCounterName, pcd.target, tags);
        Counter numIncomingMessagesCounter = SpectatorUtils.newCounter(Constants.numIncomingMessagesCounterName, pcd.target, tags);

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(queueCapacity.get());

        drainFuture = scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                if (queue.size() > 0 && ctx.channel().isWritable()) {
                    drainTriggeredCounter.increment();
                    final List<String> items = new ArrayList<>(queue.size());
                    synchronized (queue) {
                        queue.drainTo(items);
                    }
                    for (String data : items) {
                        ctx.write(Unpooled.copiedBuffer(data, StandardCharsets.UTF_8));
                        numMessagesCounter.increment();
                        numBytesCounter.increment(data.length());
                    }
                    ctx.flush();
                }
            } catch (Exception ex) {
                log.error("Error writing to channel", ex);
            }
        }, writeIntervalMillis.get(), writeIntervalMillis.get(), TimeUnit.MILLISECONDS);

        subscription = connectionBroker.connect(pcd)
                .filter(event -> !event.equalsIgnoreCase(TunnelPingMessage) || sendThroughTunnelPings)
                .doOnNext(event -> {
                    numIncomingMessagesCounter.increment();
                    if (!Constants.DUMMY_TIMER_DATA.equals(event)) {
                      String data = Constants.SSE_DATA_PREFIX + event + Constants.SSE_DATA_SUFFIX;
                        boolean offer = false;
                        synchronized (queue) {
                            offer = queue.offer(data);
                        }
                        if (!offer) {
                          numDroppedBytesCounter.increment(data.length());
                          numDroppedMessagesCounter.increment();
                      }
                    }
                })
                .subscribe();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel {} is unregistered. URI: {}", ctx.channel(), uriForLogging);
        unsubscribeIfSubscribed();
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("Channel {} is inactive. URI: {}", ctx.channel(), uriForLogging);
        unsubscribeIfSubscribed();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Exception caught by channel {}. URI: {}", ctx.channel(), uriForLogging, cause);
        unsubscribeIfSubscribed();
        // This is the tail of handlers. We should close the channel between the server and the client,
        // essentially causing the client to disconnect and terminate.
        ctx.close();
    }

    /** Unsubscribe if it's subscribed. */
    private void unsubscribeIfSubscribed() {
        if (subscription != null && !subscription.isUnsubscribed()) {
            log.info("SSE unsubscribing subscription with URI: {}", uriForLogging);
            subscription.unsubscribe();
        }
        if (drainFuture != null) {
            drainFuture.cancel(false);
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
        }
    }

    private boolean hasTunnelPingParam(String uri) {
        return uri != null && uri.contains(TunnelPingParamName);
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

    /**
     * Fetches a region from a URI if it contains one, returns garbage if not.
     *
     * @param uri The uri from which to fetch the region.
     * @return The region embedded in the URI, always lower case.
     * */
    private static String getRegion(String uri) {
        return uri.replaceFirst("^/region/", "")
                .replaceFirst("/.*$", "")
                .trim()
                .toLowerCase();
    }

    /**
     * Checks for a specific `all` string to connect to all regions
     * specified by {@link CrossRegionHandler#getTunnelRegions()}
     */
    private static boolean isAllRegion(String region) {
        return region != null && region.trim().equalsIgnoreCase("all");
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

