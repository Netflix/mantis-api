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

package io.mantisrx.api.handlers.utils;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.archaius.api.Property;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.impl.AtomicDouble;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.server.core.master.MasterDescription;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Try;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.channel.StringTransformer;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurators;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientPipelineConfigurator;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.client.HttpResponseHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


public class MantisClientUtil {


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


    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory.getLogger(MantisClientUtil.class);
    private static final String zkRootProp = "mantis.zookeeper.root";

    private static final Long apiCacheExpirySeconds = Try.of(() -> Long.parseLong(System.getProperty(PropertyNames.mantisAPICacheExpirySeconds))).getOrElse(5L);
    private static final Long apiCacheSize = Try.of(() -> Long.parseLong(System.getProperty(PropertyNames.mantisAPICacheSize))).getOrElse(1000L);


    private static final Cache<String, Tuple2<HttpResponseStatus, String>> cache = CacheBuilder.newBuilder().maximumSize(apiCacheSize).expireAfterWrite(apiCacheExpirySeconds, TimeUnit.SECONDS).build();

    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final Registry registry = Spectator.globalRegistry();
    private static final AtomicDouble cacheHitRate = SpectatorUtils.buildAndRegisterGauge(registry, "mantisapi.master.cache.hitRate");
    private static final AtomicDouble cacheSize = SpectatorUtils.buildAndRegisterGauge(registry, "mantisapi.master.cache.size");

    static {
        scheduler.scheduleAtFixedRate(() -> {
            cacheHitRate.set(cache.stats().hitRate());
            cacheSize.set(cache.size());
        }, 30, 30, TimeUnit.SECONDS);
    }

    public static boolean isSourceJobName(String jobName) {
        return jobName.endsWith("Source");
    }

    public static boolean isSourceJobId(String jobId) {
        return isSourceJobName(getJobNameFromId(jobId));
    }

    private static String getJobNameFromId(String jobId) {
        int i = jobId.lastIndexOf('-');
        return i < 0 ? jobId : jobId.substring(0, i);
    }

    public static Observable<MasterResponse> callGetOnMaster(Observable<MasterDescription> masterObservable, String endpoint, Registry registry) {
        if (logger.isDebugEnabled()) { logger.debug("Calling master-------->"); }
        SpectatorUtils.buildAndRegisterCounter(registry, "mantis.api.invoked.count", "endpoint", endpoint).increment();
        PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<String>> pipelineConfigurator
                = new HttpClientPipelineConfigurator<>();

        return masterObservable
                .filter(masterDesc -> masterDesc != null)
                .flatMap(masterDesc -> {
                    HttpClient<String, ByteBuf> client =
                            RxNetty.<String, ByteBuf>newHttpClientBuilder(masterDesc.getHostname(), masterDesc.getApiPort())
                                    .pipelineConfigurator(pipelineConfigurator)
                                    .build();
                    HttpClientRequest<String> request = HttpClientRequest.create(HttpMethod.GET, endpoint);
                    return client.submit(request)
                            .flatMap(response -> {

                                Observable<ByteBuf> bb = response.getContent()
                                        .collect(Unpooled::buffer, ByteBuf::writeBytes);

                                return Observable.just(new MasterResponse(response.getStatus(), bb, response.getHeaders()));
                            })
                            ;
                })
                .take(1)
                ;
    }

    public static Observable<Tuple2<HttpResponseStatus, String>> callGetOnMasterWithCache(Observable<MasterDescription> masterObservable, String endpoint, Property<Boolean> apiCacheEnabled) {
        if (!apiCacheEnabled.get()) {
            if (logger.isDebugEnabled()) { logger.debug("Caching disabled!"); }
            return callGetOnMaster(masterObservable, endpoint, registry)
                    .flatMap((masterResponse) -> masterResponse.getByteBuf().map(bb -> Tuple.of(masterResponse.getStatus(), bb.toString(Charset.forName("UTF-8")))));
        }
        Tuple2<HttpResponseStatus, String> result = cache.getIfPresent(endpoint);
        if (result != null) {
            if (logger.isDebugEnabled()) {logger.debug("Found in Cache " + endpoint); }
            SpectatorUtils.buildAndRegisterCounter(registry, "mantis.api.cache.hit.count", "endpoint", endpoint).increment();
            return Observable.just(result);
        } else {
            return callGetOnMaster(masterObservable, endpoint, registry)
                    .flatMap((response) -> {
                        return response.getByteBuf().map(bb -> {
                            String res = bb.toString(Charset.forName("UTF-8"));
                            if (logger.isDebugEnabled()) {logger.debug("Add to Cache " + endpoint + " result " + res); }
                            cache.put(endpoint, Tuple.of(response.getStatus(), res));
                            return Tuple.of(response.getStatus(), res);
                        });
                    });
        }
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
                    request = request.withHeader("Content-Type", "application/json");
                    request.withRawContent(content, StringTransformer.DEFAULT_INSTANCE);
                    return client.submit(request)
                            .map(response -> new MasterResponse(response.getStatus(), response.getContent(), response.getHeaders()));
                })
                .take(1)
                ;
    }

    public static Observable<MasterResponse> callPutOnMaster(Observable<MasterDescription> masterObservable, String uri, String content) {
        PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<String>> pipelineConfigurator
                = PipelineConfigurators.httpClientConfigurator();

        return masterObservable
                .filter(masterDesc -> masterDesc != null)
                .flatMap(masterDesc -> {
                    HttpClient<String, ByteBuf> client =
                            RxNetty.<String, ByteBuf>newHttpClientBuilder(masterDesc.getHostname(), masterDesc.getApiPort())
                                    .pipelineConfigurator(pipelineConfigurator)
                                    //.enableWireLogging(LogLevel.ERROR)
                                    .build();
                    HttpClientRequest<String> request = HttpClientRequest.create(HttpMethod.PUT, uri);
                    request = request.withHeader("Content-Type", "application/json");
                    request.withRawContent(content, StringTransformer.DEFAULT_INSTANCE);
                    return client.submit(request)
                            .map(response -> new MasterResponse(response.getStatus(), response.getContent(), response.getHeaders()));
                })
                .take(1)
                ;
    }

    public static Observable<MasterResponse> callOptionsOnMaster(Observable<MasterDescription> masterObservable, String endpoint) {
        PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<String>> pipelineConfigurator
                = PipelineConfigurators.httpClientConfigurator();

        return masterObservable
                .filter(Objects::nonNull)
                .flatMap(masterDesc -> {
                    HttpClient<String, ByteBuf> client =
                            RxNetty.<String, ByteBuf>newHttpClientBuilder(masterDesc.getHostname(), masterDesc.getApiPort())
                                    .pipelineConfigurator(pipelineConfigurator)
                                    .build();
                    HttpClientRequest<String> request = HttpClientRequest.create(HttpMethod.OPTIONS, endpoint);
                    return client.submit(request)
                            .flatMap(response -> {

                                Observable<ByteBuf> bb = response.getContent()
                                        .collect(Unpooled::buffer,
                                                ByteBuf::writeBytes);

                                return Observable.just(new MasterResponse(response.getStatus(), bb, response.getHeaders()));
                            })
                            ;
                })
                .take(1)
                ;
    }

    public static Observable<MasterResponse> callDeleteOnMaster(Observable<MasterDescription> masterObservable, String endpoint) {
        PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<String>> pipelineConfigurator
                = PipelineConfigurators.httpClientConfigurator();

        return masterObservable
                .filter(Objects::nonNull)
                .flatMap(masterDesc -> {
                    HttpClient<String, ByteBuf> client =
                            RxNetty.<String, ByteBuf>newHttpClientBuilder(masterDesc.getHostname(), masterDesc.getApiPort())
                                    .pipelineConfigurator(pipelineConfigurator)
                                    .build();
                    HttpClientRequest<String> request = HttpClientRequest.create(HttpMethod.DELETE, endpoint);
                    return client.submit(request)
                            .flatMap(response -> {

                                Observable<ByteBuf> bb = response.getContent()
                                        .collect(Unpooled::buffer,
                                                ByteBuf::writeBytes);

                                return Observable.just(new MasterResponse(response.getStatus(), bb, response.getHeaders()));
                            })
                            ;
                })
                .take(1)
                ;
    }

    public static boolean isJobId(String s) {
        if (s == null)
            return false;
        if (s.startsWith("Error "))
            return false;
        if (s.contains(" ".subSequence(0, 1)))
            return false;
        int i = s.lastIndexOf('-');
        if (i < 0 || i == s.length() - 1)
            return false;
        try (final Scanner longScanner = new Scanner(s.substring(i + 1))) {
            return longScanner.hasNextLong();
        }
    }
}
