/*
 * Copyright 2019 Netflix, Inc.
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

package io.mantisrx.api.handlers.servlets;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.MantisClientUtil;
import io.mantisrx.api.handlers.utils.QueryParams;
import io.mantisrx.api.handlers.utils.Regions;
import io.mantisrx.api.handlers.utils.RetryUtils;
import io.mantisrx.api.tunnel.StreamingClientFactory;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vavr.control.Try;
import mantis.io.reactivex.netty.channel.StringTransformer;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action2;
import rx.functions.Func0;
import rx.functions.Func1;


public class EndpointRelay implements MantisAPIRequestHandler {

    private static final Long crossRegionAPICacheSize = Try.of(() -> Long.parseLong(System.getProperty(PropertyNames.crossRegionCacheSize))).getOrElse(1000L);
    private static final Long crossRegionAPICacheExpirySeconds = Try.of(() -> Long.parseLong(System.getProperty(PropertyNames.crossRegionCacheExpirySeconds))).getOrElse(5L);

    private static final Cache<String, RegionData> crossRegionCache = CacheBuilder.newBuilder().maximumSize(crossRegionAPICacheSize).expireAfterWrite(crossRegionAPICacheExpirySeconds, TimeUnit.SECONDS).build();

    static class RegionData {

        final String region;
        final boolean success;
        final String data;
        final int responseCode;

        public RegionData(String region, boolean success, String data, int responseCode) {
            this.region = region;
            this.success = success;
            this.data = data;
            this.responseCode = responseCode;
        }
    }

    private static Logger logger = LoggerFactory.getLogger(EndpointRelay.class);
    private static final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic =
            RetryUtils.getRetryFunc(logger, 5);
    private final MasterClientWrapper masterClientWrapper;
    private final StreamingClientFactory streamingClientFactory;
    private static final Registry registry = Spectator.globalRegistry();
    private transient final PropertyRepository propertyRepository;
    private transient final WorkerThreadPool workerThreadPool;

    public EndpointRelay(MantisClient mantisClient, MasterClientWrapper masterClientWrapper, StreamingClientFactory streamingClientFactory, PropertyRepository propertyRepository, Registry registry, WorkerThreadPool workerThreadPool) {
        this.masterClientWrapper = masterClientWrapper;
        this.streamingClientFactory = streamingClientFactory;
        this.propertyRepository = propertyRepository;
        this.workerThreadPool = workerThreadPool;
    }

    @Override
    public String getName() {
        return "relay";
    }

    @Override
    public String getHelp() {
        return null;
    }

    @Override
    public Observable<Void> handleOptions(HttpServerResponse<ByteBuf> response, Request request) {
        HttpUtils.setBaseHeaders(response.getHeaders(), HttpMethod.GET, HttpMethod.POST, HttpMethod.OPTIONS);
        response.setStatus(HttpResponseStatus.NO_CONTENT);
        return response.close();
    }

    @Override
    public Observable<Void> handleGet(HttpServerResponse<ByteBuf> response, Request request) {
        logger.info("Relaying GET URI " + request.uri);
        // TODO: Three locations in this file, need to verify default should be false
        Property<Boolean> apiCacheEnabled = propertyRepository.get(PropertyNames.mantisAPICacheEnabled, Boolean.class).orElse(false);
        return MantisClientUtil
                .callGetOnMasterWithCache(masterClientWrapper.getMasterMonitor().getMasterObservable(), request.uri, apiCacheEnabled)
                .retryWhen(retryLogic)
                .doOnError(t -> {
                    logger.warn("Timed out relaying request for session id " + request.context.getId() +
                            " url=" + request.uri);
                    response.setStatus(HttpResponseStatus.REQUEST_TIMEOUT);
                    response.writeStringAndFlush(t.getMessage());
                    response.close();
                })
                .flatMap(res -> {
                    HttpUtils.setBaseHeaders(response.getHeaders(), HttpMethod.GET, HttpMethod.POST, HttpMethod.OPTIONS);
                    response.setStatus(res._1());
                    response.writeStringAndFlush(res._2());
                    return response.close();
                });
    }

    @Override
    public Observable<Void> handlePost(HttpServerResponse<ByteBuf> response, Request request) {
        return MantisClientUtil
                .callPostOnMaster(masterClientWrapper.getMasterMonitor().getMasterObservable(), request.uri, request.content)
                .retryWhen(retryLogic)
                .flatMap(masterResponse -> {
                    response.setStatus(masterResponse.getStatus());
                    HttpUtils.setBaseHeaders(response.getHeaders(), HttpMethod.GET, HttpMethod.POST, HttpMethod.OPTIONS);
                    return masterResponse.getByteBuf()
                            .take(1)
                            .flatMap(byteBuf -> {
                                        response.writeStringAndFlush(byteBuf.toString(Charset.forName("UTF-8")));
                                        return response.close();
                                    }
                            );
                });
    }

    static String getUri(String reqUri, String queryString) {
        StringBuilder b = new StringBuilder("/").append(reqUri);
        if (queryString != null && !queryString.isEmpty())
            b.append("?").append(queryString).append("&").append(QueryParams.getTunnelConnectParams());
        else
            b.append("?").append(QueryParams.getTunnelConnectParams());
        return b.toString();
    }

    static Observable<RegionData> handleSubmitResponse(StreamingClientFactory streamingClientFactory, long sessionId, HttpClientRequest<String> rq, String region) {
        final AtomicReference<Throwable> ref = new AtomicReference<>();

        SpectatorUtils.buildAndRegisterCounter(registry, "mantis.api.cross.invoked.count", "region", region, "endpoint", rq.getUri(), "method", String.valueOf(rq.getMethod())).increment();

        return Observable
                .create((Observable.OnSubscribe<HttpClient<String, ByteBuf>>) subscriber ->
                        subscriber.onNext(streamingClientFactory.getSimpleSecureClient(region)))
                .flatMap(client -> {
                    if (client == null)
                        return Observable.just(new RegionData(region, false, "Can't connect to remote master via tunnel", 0));
                    ref.set(null);
                    return client.submit(rq)
                            .flatMap(resp -> {
                                final int code = resp.getStatus().code();
                                if (code >= 500) {
                                    throw new RuntimeException(resp.getStatus().toString());
                                }
                                return resp.getContent()
                                        .collect((Func0<ByteBuf>) Unpooled::buffer,
                                                (Action2<ByteBuf, ByteBuf>) ByteBuf::writeBytes)
                                        .map(byteBuf -> new RegionData(region, true,
                                                byteBuf.toString(Charset.forName("UTF-8")), code)
                                        )
                                        .onErrorReturn(t -> new RegionData(region, false, t.getMessage(), code))
                                        ;
                            })
                            .onErrorReturn(t -> {
                                logger.warn("Error getting response from remote master: " + t.getMessage());
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
                .retryWhen(retryLogic)
                .take(1)
                .onErrorReturn(t -> new RegionData(region, false, t.getMessage(), 0))
                ;
    }

    static Observable<RegionData> handleSubmitResponseForGetWithCache(StreamingClientFactory streamingClientFactory, long sessionId, HttpClientRequest<String> rq, String region, Property<Boolean> enableCrossRegionApiCache) {
        if (!enableCrossRegionApiCache.get() || !rq.getMethod().equals(HttpMethod.GET)) {
            return handleSubmitResponse(streamingClientFactory, sessionId, rq, region);
        }
        String cacheKey = region + "_" + rq.getUri();
        RegionData result = crossRegionCache.getIfPresent(cacheKey);
        if (result != null) {
            SpectatorUtils.buildAndRegisterCounter(registry, "mantis.api.cross.cache.hit.count", "region", region, "endpoint", rq.getUri()).increment();
            return Observable.just(result);
        } else {
            return handleSubmitResponse(streamingClientFactory, sessionId, rq, region).doOnNext((rData) -> crossRegionCache.put(cacheKey, rData));
        }
    }


    private void writeResults(HttpServerResponse<ByteBuf> response, List<RegionData> dataList) {
        HttpUtils.setBaseHeaders(response.getHeaders(), HttpMethod.GET, HttpMethod.POST, HttpMethod.OPTIONS);
        response.getHeaders().set("content-type", "application/json");
        response.setStatus(HttpResponseStatus.OK);
        response.writeString("[");
        boolean first = true;
        for (RegionData data : dataList) {
            if (first)
                first = false;
            else
                response.writeStringAndFlush(", ");
            if (data.success)
                response.writeStringAndFlush(Regions.getForceWrappedJson(data.data, data.region, data.responseCode, null));
            else
                response.writeStringAndFlush(Regions.getForceWrappedJson("", data.region, data.responseCode, data.data));
        }
        response.writeStringAndFlush("]");
    }

    Observable<Void> handleGetWithRegions(HttpServerResponse<ByteBuf> response, Request request, String path) {
        logger.info("Relaying GET URI " + path);
        List<String> regions = request.context.getRegions();
        if (regions == null || regions.isEmpty())
            regions = Lists.newArrayList(Regions.getLocalRegion());
        return Observable.from(regions)
                .flatMap(region -> {
                    if (Regions.isLocalRegion(region)) {
                        String uri = path;
                        if (request.queryString != null && !request.queryString.isEmpty())
                            uri = path + "?" + request.queryString;

                        Property<Boolean> apiCacheEnabled = propertyRepository.get(PropertyNames.mantisAPICacheEnabled, Boolean.class).orElse(false);
                        return MantisClientUtil
                                .callGetOnMasterWithCache(masterClientWrapper.getMasterMonitor().getMasterObservable(), "/" + uri, apiCacheEnabled)
                                .retryWhen(retryLogic)
                                .map(result -> new RegionData(region, true, result._2(), result._1().code()))
                                .onErrorReturn(t -> {
                                    logger.warn("Error on getting response from local master: " + t.getMessage(), t);
                                    return new RegionData(region, false, t.getMessage(), 0);
                                })
                                ;
                    } else {
                        final String uri = getUri(path, request.queryString);
                        HttpClientRequest<String> rq = HttpClientRequest.create(HttpMethod.GET, uri);
                        Property<Boolean> crossRegionCacheEnabled = propertyRepository.get(PropertyNames.crossRegionCacheEnabled, Boolean.class).orElse(false);
                        return handleSubmitResponseForGetWithCache(streamingClientFactory, request.context.getId(), rq, region, crossRegionCacheEnabled);
                    }
                })
                .reduce(new ArrayList<RegionData>(), (regionDatas, regionData) -> {
                    regionDatas.add(regionData);
                    return regionDatas;
                })
                .flatMap(dataList -> {
                    writeResults(response, dataList);
                    return response.close();
                });
    }

    Observable<Void> handlePostWithRegions(HttpServerResponse<ByteBuf> response, Request request, String path) {
        logger.info("Relaying POST URI " + path);
        List<String> regions = request.context.getRegions();
        if (regions == null || regions.isEmpty())
            regions = Lists.newArrayList(Regions.getLocalRegion());
        return Observable.from(regions)
                .flatMap(region -> {
                    if (Regions.isLocalRegion(region)) {
                        String uri = path;
                        if (request.queryString != null && !request.queryString.isEmpty())
                            uri = path + "?" + request.queryString;
                        return MantisClientUtil
                                .callPostOnMaster(masterClientWrapper.getMasterMonitor().getMasterObservable(), "/" + uri, request.content)
                                .retryWhen(retryLogic)
                                .flatMap(masterResponse -> {
                                    int code = masterResponse.getStatus().code();
                                    return masterResponse.getByteBuf()
                                            .take(1)
                                            .map(byteBuf -> new RegionData(region, true, byteBuf.toString(Charset.forName("UTF-8")), code))
                                            .onErrorReturn(t -> {
                                                logger.warn("Error on POST to local master: " + t.getMessage(), t);
                                                return new RegionData(region, false, t.getMessage(), code);
                                            });
                                })
                                .onErrorReturn(t -> {
                                    logger.warn("Error on POST to local master: " + t.getMessage(), t);
                                    return new RegionData(region, false, t.getMessage(), 0);
                                })
                                ;
                    } else {
                        final String uri = getUri(path, request.queryString);
                        HttpClientRequest<String> rq = HttpClientRequest.create(HttpMethod.POST, uri);
                        rq.withRawContent(request.content, StringTransformer.DEFAULT_INSTANCE);
                        return handleSubmitResponse(streamingClientFactory, request.context.getId(), rq, region);
                    }
                })
                .reduce(new ArrayList<RegionData>(), (regionDatas, regionData) -> {
                    regionDatas.add(regionData);
                    return regionDatas;
                })
                .flatMap(dataList -> {
                    writeResults(response, dataList);
                    return response.close();
                });
    }
}
