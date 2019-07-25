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

package io.mantisrx.api.handlers.connectors;

import java.io.UnsupportedEncodingException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.impl.AtomicDouble;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.api.handlers.servlets.JobConnectById;
import io.mantisrx.api.handlers.servlets.JobConnectByName;
import io.mantisrx.api.handlers.servlets.MantisAPIRequestHandler;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.MantisClientUtil;
import io.mantisrx.api.handlers.utils.QueryParams;
import io.mantisrx.api.handlers.utils.Regions;
import io.mantisrx.api.handlers.utils.RetryUtils;
import io.mantisrx.api.handlers.utils.TunnelUtils;
import io.mantisrx.api.metrics.Stats;
import io.mantisrx.client.MantisClient;
import io.mantisrx.client.SinkConnectionFunc;
import io.mantisrx.client.SseSinkConnectionFunction;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.MantisJobState;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vavr.control.Try;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;


public class JobSinkConnector {

    private static final Logger logger = LoggerFactory.getLogger(JobSinkConnector.class);
    public static final String MetaErrorMsgHeader = "mantis.meta.error.message";
    public static final String MetaErrorMsgPrefix = "Mantis.Meta.Error: ";


    private static final String DUMMY_TIMER_DATA = "DUMMY_TIMER_DATA";
    private static final String TWO_NEWLINES = "\n\n";
    private static final String SSE_DATA_PREFIX = "data: ";
    private final MantisClient mantisClient;
    private static final long flushIntervalMillis = 50;
    private final boolean isJobId;
    private static final String numMessagesCounterName = "numSinkMessages";
    private static final String numDroppedMessagesCounterName = "numDroppedSinkMessages";
    private static final String numBytesCounterName = "numSinkBytes";
    private static final String numDroppedBytesCounterName = "numDroppedSinkBytes";
    private static final String numUnexpectedRemoteConxDrop = "numUnexpectedRemoteConxDrop";
    private static final String numUnexpectedConxDrop = "numUnexpectedConxDrop";
    private static final String numExpectedRegionalConx = "numExpectedRegionalConx";
    private static final String numConnectedRegionalConx = "numConnectedRegionalConx";
    private static long minSampleValue = Try.of(() -> Long.parseLong(System.getProperty(PropertyNames.samplingInterval))).getOrElse(250L);
    private final static String sampleParamName = "sampleMSec";
    private final RemoteSinkConnector remoteSinkConnector;
    private static final Registry registry = Spectator.globalRegistry();
    private static final int CAPACITY = 5000;
    private final ScheduledThreadPoolExecutor threadPoolExecutor;

    private final Property<Boolean> sourceSamplingenabled;

    public JobSinkConnector(MantisClient mantisClient, boolean isJobId, RemoteSinkConnector remoteSinkConnector, PropertyRepository propertyRepository, ScheduledThreadPoolExecutor threadpool) {
        this.mantisClient = mantisClient;
        this.isJobId = isJobId;
        this.remoteSinkConnector = remoteSinkConnector;
        this.threadPoolExecutor = threadpool;

        this.sourceSamplingenabled = propertyRepository.get(PropertyNames.sourceSamplingEnabled, Boolean.class).orElse(false);
    }

    public Observable<Void> connect(String target, HttpServerResponse<ByteBuf> response, MantisAPIRequestHandler.Request request) {
        if (target == null || target.isEmpty()) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return response.writeStringAndFlush("Must provide " + (isJobId ? "jobId" : "jobName") + " in URI");
        }
        boolean isSourceJob = isJobId ?
                MantisClientUtil.isSourceJobId(target) :
                MantisClientUtil.isSourceJobName(target);
        Map<String, List<String>> queryParameters = request.queryParameters;
        try {
            final SinkParameters sinkParameters = getSinkParameters(queryParameters, isSourceJob, sourceSamplingenabled);
            Func0<Observable<Observable<MantisServerSentEvent>>> localResultsGetter =
                    () -> getResults(isJobId, mantisClient, target, sinkParameters);
            Func1<String, Observable<MantisServerSentEvent>> remoteResultsGetter = region ->
                    Observable.create(new Observable.OnSubscribe<MantisServerSentEvent>() {
                        @Override
                        public void call(Subscriber<? super MantisServerSentEvent> subscriber) {
                            remoteSinkConnector.getResults(
                                    region,
                                    isJobId ? JobConnectById.handlerName : JobConnectByName.handlerName,
                                    target,
                                    sinkParameters
                            )
                                    .subscribe(subscriber);
                        }
                    });
            return process(response, request, localResultsGetter, remoteResultsGetter, threadPoolExecutor);
        } catch (UnsupportedEncodingException e) {
            final String error = String.format("Error in sink connect request's queryParams [%s], error: %s",
                    queryParameters, e.getMessage());
            logger.error(error, e);
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return response.writeStringAndFlush(error);
        }
    }

    public static SinkParameters getSinkParameters(Map<String, List<String>> queryParameters, boolean downSample, Property<Boolean> sourceSamplingEnabled) throws UnsupportedEncodingException {
        boolean foundSampleParam = false;

        boolean dSample = downSample;
        // if source autosampling is disabled
        if (!sourceSamplingEnabled.get()) {
            dSample = false;
        }
        SinkParameters.Builder builder = new SinkParameters.Builder();
        if (queryParameters != null && !queryParameters.isEmpty()) {
            for (Map.Entry<String, List<String>> entry : queryParameters.entrySet()) {
                for (String v : entry.getValue()) {
                    if (dSample) {
                        if (sampleParamName.equals(entry.getKey())) {
                            if (isSamplingValueOK(v)) {
                                builder.withParameter(entry.getKey(), v);
                                foundSampleParam = true;
                            }
                        } else
                            builder.withParameter(entry.getKey(), v);
                    } else
                        builder.withParameter(entry.getKey(), v);
                }
            }
        }
        if (dSample && !foundSampleParam) {
            logger.info("Downsampling for source job connector");
            builder.withParameter(sampleParamName, "250");
        }
        return builder.build();
    }

    private static boolean isSamplingValueOK(String v) {
        try {
            return Long.parseLong(v) >= minSampleValue;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static Observable<Observable<String>> getRetryWrappedLocalObservable(Observable<Observable<String>> o) {
        final AtomicBoolean retriable = new AtomicBoolean(true);
        final Func1<Observable<? extends Throwable>, Observable<?>> conditional =
                RetryUtils.getRetryFuncConditional(logger, Integer.MAX_VALUE, retriable);
        return o
                .onErrorResumeNext(t -> {
                    logger.warn(t.getMessage());
                    if (MasterClientWrapper.InvalidNamedJob.equals(t.getMessage())) {
                        retriable.set(false);
                        return Observable.just(Observable.just(MetaErrorMsgPrefix + t.getMessage()),
                                Observable.timer(1, TimeUnit.SECONDS).take(1).flatMap(l -> Observable.<String>error(t)));
                    } else {
                        return Observable.error(t);
                    }
                })
                .retryWhen(conditional)
                ;
    }

    public static Observable<Observable<String>> composeCrossRegionObservables(
            List<String> regions, boolean sendTnlPings,
            PublishSubject<Integer> untilSubject,
            Func0<Observable<Observable<MantisServerSentEvent>>> localResultsGetter,
            Func1<String, Observable<MantisServerSentEvent>> remoteResultsGetter, List<Tag> tags, long sessionId) {

        AtomicDouble expectedRegionalConnectionGauge = SpectatorUtils.buildAndRegisterGauge(registry, getSessionSuffixedMetricName(numExpectedRegionalConx, sessionId), tags);
        Observable<Observable<String>> results;
        if (regions != null && !regions.isEmpty()) {
            List<Observable<Observable<String>>> resultsList = new ArrayList<>();
            final AtomicReference<AtomicInteger> ref = new AtomicReference<>();
            for (String region : regions) {
                if (Regions.isLocalRegion(region)) {
                    PublishSubject<Integer> localUntil = PublishSubject.create();
                    final Func0<Observable<Observable<String>>> observableF =
                            () -> localResultsGetter.call()
                                    .map(o -> o.map(msse -> {
                                        if (msse.getEventAsString().startsWith(MetaErrorMsgPrefix)) {
                                            Schedulers.computation().createWorker().schedule(
                                                    () -> {
                                                        localUntil.onNext(1);
                                                        if (ref.get().decrementAndGet() == 0)
                                                            untilSubject.onNext(1);
                                                    },
                                                    2, TimeUnit.SECONDS
                                            );
                                            return Regions.getWrappedJson("{}", region, msse.getEventAsString());
                                        }
                                        return Regions.getWrappedJson(msse.getEventAsString(), region, "");
                                    }));
                    resultsList.add(getRetryWrappedRemoteObservable(
                            () -> {
                                logger.info("Session " + sessionId + " connecting to local observable");
                                return observableF.call().flatMap(o -> o);
                            },
                            localUntil.delay(1, TimeUnit.SECONDS),
                            sessionId, region, tags
                    ));
                } else {
                    PublishSubject<Integer> regionUntil = PublishSubject.create();
                    resultsList.add(
                            getRetryWrappedRemoteObservable(
                                    () -> {
                                        logger.info("Session id " + sessionId + " connecting to region: " + region);
                                        return remoteResultsGetter.call(region)
                                                .map(msse -> {
                                                    if (msse.getEventAsString().startsWith(MetaErrorMsgPrefix)) {
                                                        Schedulers.computation().createWorker().schedule(
                                                                () -> {
                                                                    regionUntil.onNext(1);
                                                                    if (ref.get().decrementAndGet() == 0)
                                                                        untilSubject.onNext(1);
                                                                },
                                                                2, TimeUnit.SECONDS
                                                        );
                                                        return Regions.getWrappedJson("{}", region, msse.getEventAsString());
                                                    }
                                                    return Regions.getWrappedJson(msse.getEventAsString(), region, "");
                                                });
                                    },
                                    regionUntil.delay(1, TimeUnit.SECONDS),
                                    sessionId, region, tags
                            ));
                }
            }
            ref.set(new AtomicInteger(resultsList.size()));

            expectedRegionalConnectionGauge.set(resultsList.size());

            results = Observable
                    .from(resultsList)
                    .flatMap(o -> o)
                    .takeUntil(untilSubject)
                    .doAfterTerminate(() -> expectedRegionalConnectionGauge.set(0));
        } else {
            results = localResultsGetter.call()
                    .map(o -> o.map(MantisServerSentEvent::getEventAsString));
            if (sendTnlPings)
                results = results.mergeWith(Observable.just(
                        Observable.interval(TunnelUtils.TunnelPingIntervalSecs, TunnelUtils.TunnelPingIntervalSecs,
                                TimeUnit.SECONDS)
                                .map(l -> TunnelUtils.TunnelPingMessage)
                ));
            results = getRetryWrappedLocalObservable(results);

        }
        return results;
    }

    private static String getSessionSuffixedMetricName(String m, long id) {
        return m + "-" + id;
    }

    private static Observable<Observable<String>> getRetryWrappedRemoteObservable(
            Func0<Observable<String>> oFunc,
            Observable<Integer> remoteUntil,
            long sessionId, String region,
            List<Tag> tags) {
        AtomicBoolean isConnected = new AtomicBoolean();


        AtomicDouble remoteRegionConxGauge = SpectatorUtils.buildAndRegisterGauge(registry, getSessionSuffixedMetricName(numConnectedRegionalConx, sessionId), tags);
        Counter unexpectedConxDrops = SpectatorUtils.buildAndRegisterCounter(registry, numUnexpectedConxDrop, tags);
        Counter unexpectedRemoteConxDrops = SpectatorUtils.buildAndRegisterCounter(registry, numUnexpectedRemoteConxDrop, tags);

        return Observable.interval(2, TimeUnit.SECONDS)
                .takeUntil(remoteUntil)
                .filter(l -> !isConnected.get())
                .map(l -> {
                    logger.info("Session id " + sessionId + " Connecting to remote observable, region=" + region);
                    isConnected.set(true);
                    remoteRegionConxGauge.set(1.0);

                    return oFunc.call()
                            .takeUntil(remoteUntil)
                            .onErrorResumeNext(t -> {
                                isConnected.set(false);
                                remoteRegionConxGauge.set(0.0);
                                if (Regions.isLocalRegion(region))
                                    unexpectedConxDrops.increment();
                                else
                                    unexpectedRemoteConxDrops.increment();
                                logger.warn("Session id " + sessionId + " : retrying after error with remote (" + region +
                                        ") observable: " + t.getMessage());
                                return Observable.empty();
                            })
                            .doOnCompleted(() -> {
                                isConnected.set(false);
                                remoteRegionConxGauge.set(0.0);
                                if (Regions.isLocalRegion(region))
                                    unexpectedConxDrops.increment();
                                else
                                    unexpectedRemoteConxDrops.increment();
                                logger.warn("Session id " + sessionId + " : retrying unexpected complete of remote observable, region=" + region);
                            })
                            ;
                });
    }

    public static Observable<Void> process(HttpServerResponse<ByteBuf> response, final MantisAPIRequestHandler.Request request,
                                           Func0<Observable<Observable<MantisServerSentEvent>>> localResultsGetter,
                                           Func1<String, Observable<MantisServerSentEvent>> remoteResultsGetter,
                                           ScheduledThreadPoolExecutor threadpool) {

        boolean sendTunnelPings = request.queryParameters != null &&
                request.queryParameters.get(QueryParams.TunnelPingParamName) != null &&
                Boolean.valueOf(request.queryParameters.get(QueryParams.TunnelPingParamName).get(0));
        final PublishSubject<Integer> untilSubject = PublishSubject.create();
        Observable<Observable<String>> results = composeCrossRegionObservables(request.context.getRegions(),
                sendTunnelPings, untilSubject, localResultsGetter, remoteResultsGetter, request.tags, request.context.getId()
        );
        setHeadersAndFlush(response);
        return intlWriteResults(results, response, request.context.getStats(), request.tags, untilSubject, threadpool);
    }

    private static Observable<Void> intlWriteResults(Observable<Observable<String>> results, HttpServerResponse<ByteBuf> response,
                                                     Stats stats, List<Tag> tags, Observable<Integer> untilSubject,
                                                     ScheduledThreadPoolExecutor threadpool) {
        final AtomicBoolean hasErrored = new AtomicBoolean(false);
        final AtomicBoolean hasClosed = new AtomicBoolean(false);
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>(CAPACITY);
        final AtomicReference<Exception> errorRef = new AtomicReference<>();
        final ScheduledFuture<?> writerFuture = startWriter(hasClosed, response, queue, tags, errorRef, stats, threadpool);
        response.getChannel().closeFuture().addListener(future -> {
            logger.info("Disconnect on netty channel");
            hasClosed.set(true);
        });
        return results
                .doOnError(throwable -> {
                    logger.warn(throwable.getMessage());
                    response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
                    response.getHeaders().addHeader(MetaErrorMsgHeader, throwable.getMessage());
                    response.writeStringAndFlush(throwable.getMessage());
                    response.close();
                    hasErrored.set(true);
                })
                .mergeWith(
                        Observable.interval(5, 5, TimeUnit.SECONDS)
                                .map(aLong -> Observable.just(DUMMY_TIMER_DATA))
                )
                .flatMap(eventObservable -> eventObservable
                        .buffer(flushIntervalMillis, TimeUnit.MILLISECONDS)
                        .flatMap(valueList -> {
                            if (response.isCloseIssued() || !response.getChannel().isActive()) {
                                return Observable.error(new ClosedChannelException());
                            }
                            if (errorRef.get() != null) {
                                hasErrored.set(true);
                                return Observable.error(errorRef.get());
                            }
                            long d = 0L;
                            long dBytes = 0L;
                            for (String s : valueList) {
                                if (!queue.offer(s) && !DUMMY_TIMER_DATA.equals(s)) {
                                    d++;
                                    dBytes += s.length();
                                }
                            }
                            if (d > 0)
                                updateDroppedMetrics(d, dBytes, tags, stats, registry);
                            return Observable.<Void>empty();
                        }))
                .takeWhile(eventObservable -> !hasErrored.get() && !hasClosed.get())
                .takeUntil(untilSubject)
                .doOnUnsubscribe(() -> tearDown(writerFuture, queue))
                .doOnCompleted(() -> tearDown(writerFuture, queue));
    }

    // TODO: Does this function need to exist?
    //       It is called in two locations, both static, which is a pain because of Registry
    private static void updateDroppedMetrics(long msgs, long bytes, List<Tag> tags, Stats stats, Registry registry) {
        stats.incrementDroppedMessages(msgs);
        stats.incrementDroppedBytes(bytes);

        SpectatorUtils.buildAndRegisterCounter(registry, numDroppedMessagesCounterName, tags).increment(msgs);
        SpectatorUtils.buildAndRegisterCounter(registry, numDroppedBytesCounterName, tags).increment(bytes);
    }

    private static void tearDown(ScheduledFuture<?> writerFuture, BlockingQueue<String> queue) {
        writerFuture.cancel(true);
        queue.clear();
    }

    private static ScheduledFuture<?> startWriter(AtomicBoolean hasClosed, HttpServerResponse<ByteBuf> response, BlockingQueue<String> queue,
                                                  List<Tag> tags, AtomicReference<Exception> errorRef, Stats stats, ScheduledThreadPoolExecutor threadPoolExecutor) {

        Counter numMessagesCounter = SpectatorUtils.buildAndRegisterCounter(registry, numMessagesCounterName, tags);
        Counter numBytesCounter = SpectatorUtils.buildAndRegisterCounter(registry, numBytesCounterName, tags);

        return threadPoolExecutor.scheduleWithFixedDelay(() -> {
            if (hasClosed.get())
                return;
            final List<String> items = new ArrayList<>(CAPACITY);
            queue.drainTo(items);
            boolean written = false;
            int noOfMsgs = 0;
            StringBuilder sb = new StringBuilder(items.size() * 3);
            for (String s : items) {
                if (s.startsWith(MetaErrorMsgPrefix)) {
                    errorRef.set(new Exception(s.substring(MetaErrorMsgHeader.length())));
                    return;
                }
                if (!DUMMY_TIMER_DATA.equals(s)) {
                    sb.append(SSE_DATA_PREFIX);
                    sb.append(s);
                    sb.append(TWO_NEWLINES);
                    noOfMsgs++;
                }
            }
            int maxTries = 100;
            int trial = 0;
            if (sb.length() > 0) {
                while (!written && ++trial < maxTries) {
                    if (response.getChannel().isWritable()) {
                        stats.incrementNumMessages(noOfMsgs);
                        numMessagesCounter.increment(noOfMsgs);
                        stats.incrementNumBytes(sb.length());
                        numBytesCounter.increment(sb.length());
                        response.writeStringAndFlush(sb.toString());
                        written = true;
                    } else {
                        // sleep for a bit before retrying
                        try {
                            Thread.sleep(flushIntervalMillis);
                        } catch (InterruptedException e) {
                            // safe to ignore
                        }
                    }
                }
                if (!written) {
                    updateDroppedMetrics(noOfMsgs, sb.length(), tags, stats, registry);
                }
            }
        }, flushIntervalMillis, flushIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private static void setHeadersAndFlush(HttpServerResponse<ByteBuf> response) {
        HttpUtils.setBaseHeaders(response.getHeaders(), HttpMethod.GET, HttpMethod.OPTIONS);
        HttpUtils.setSseHeaders(response.getHeaders());
        response.flush();
    }

    private static SinkConnectionFunc<MantisServerSentEvent> getSseConnFunc(final String target, SinkParameters sinkParameters) {
        return new SseSinkConnectionFunction(true,
                t -> logger.warn("Reconnecting to sink of job " + target + " after error: " + t.getMessage()),
                sinkParameters);
    }

    public static Observable<Observable<MantisServerSentEvent>> getResults(boolean isJobId, MantisClient mantisClient,
                                                                           final String target, SinkParameters sinkParameters) {

        final AtomicBoolean hasError = new AtomicBoolean();
        return  isJobId ?
                mantisClient.getSinkClientByJobId(target, getSseConnFunc(target, sinkParameters), null).getResults() :
                mantisClient.getSinkClientByJobName(target, getSseConnFunc(target, sinkParameters), null)
                        .switchMap(serverSentEventSinkClient -> {
                            if (serverSentEventSinkClient.hasError()) {
                                hasError.set(true);
                                return Observable.error(new Exception(serverSentEventSinkClient.getError()));
                            }
                            return serverSentEventSinkClient.getResults();
                        })
                        .takeWhile(o -> !hasError.get());
    }
}
