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

package io.mantisrx.api.handlers.ws;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.impl.AtomicDouble;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.ServletConx;
import io.mantisrx.api.handlers.connectors.JobSinkConnector;
import io.mantisrx.api.handlers.connectors.RemoteSinkConnector;
import io.mantisrx.api.handlers.servlets.JobConnectById;
import io.mantisrx.api.handlers.servlets.JobConnectByIdWebSocketServlet;
import io.mantisrx.api.handlers.servlets.JobConnectByName;
import io.mantisrx.api.handlers.utils.MantisClientUtil;
import io.mantisrx.api.handlers.utils.QueryParams;
import io.mantisrx.api.metrics.Stats;
import io.mantisrx.client.MantisClient;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SinkParameters;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.subjects.PublishSubject;


public class JobConnectWebSocket extends WebSocketAdapter {

    private static Logger logger = LoggerFactory.getLogger(JobConnectWebSocket.class);
    private final boolean isJobId;
    private final MantisClient mantisClient;
    private String target;
    private final Map<String, List<String>> qryParams;
    private static long flushIntervalMillis = 50;
    private final SessionContext sessionContext;
    private volatile Subscription subscription = null;
    private volatile AtomicBoolean closed = new AtomicBoolean(false);
    private final Stats stats;
    private final List<Tag> tags;
    private final Registry registry;
    private final WorkerThreadPool workerThreadPool;
    private final PropertyRepository propertyRepository;

    private static final String DUMMY_TIMER_DATA = "DUMMY_TIMER_DATA";
    private static final String numMessagesCounterName = "numSinkMessages";
    private static final String numDroppedMessagesCounterName = "numDroppedSinkMessages";
    private static final String numBytesCounterName = "numSinkBytes";
    private static final String numDroppedBytesCounterName = "numDroppedSinkBytes";
    private static final String wsQueueSizeGaugeName = "wsQueueSize";

    private final AtomicDouble wsQueueSizeGauge;
    private final Counter enpointConnectionCounter;

    private final RemoteSinkConnector remoteSinkConnector;
    private SinkParameters sinkParameters = null;
    private final boolean sendTunnelPings;

    public JobConnectWebSocket(boolean isJobId, MantisClient mantisClient, Stats stats, Map<String, List<String>> qryParams, SessionContext sessionContext, RemoteSinkConnector remoteSinkConnector, Registry registry, PropertyRepository propertyRepository, WorkerThreadPool workerThreadPool) {
        this.isJobId = isJobId;
        this.mantisClient = mantisClient;
        this.stats = stats;
        this.qryParams = qryParams;
        this.sessionContext = sessionContext;
        this.remoteSinkConnector = remoteSinkConnector;
        this.workerThreadPool = workerThreadPool;
        this.propertyRepository = propertyRepository;
        sendTunnelPings = qryParams != null && qryParams.get(QueryParams.TunnelPingParamName) != null &&
                Boolean.valueOf(qryParams.get(QueryParams.TunnelPingParamName).get(0));
        tags = QueryParams.getTaglist(qryParams, sessionContext.getId(), sessionContext.getUri());

        // Metrics

        this.registry = registry;
        this.wsQueueSizeGauge = SpectatorUtils.buildAndRegisterGauge(registry, "wsQueueSize");
        this.enpointConnectionCounter = SpectatorUtils.buildAndRegisterCounter(registry, JobConnectByIdWebSocketServlet.endpointName);
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public void setSinkParameters() throws UnsupportedEncodingException {
        sinkParameters = JobSinkConnector.getSinkParameters(
                qryParams,
                isJobId ? MantisClientUtil.isSourceJobId(target) : MantisClientUtil.isSourceJobName(target),
                propertyRepository.get(PropertyNames.sourceSamplingEnabled, Boolean.class).orElse(false));
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        super.onWebSocketClose(statusCode, reason);
        closed.set(true);
        if (subscription != null) {
            subscription.unsubscribe();
        }
        sessionContext.endSession();
    }

    @Override
    public void onWebSocketConnect(Session sess) {
        super.onWebSocketConnect(sess);
        this.enpointConnectionCounter.increment();

        if (target == null || target.isEmpty()) {
            String error = "Must provide " + (isJobId ? "jobId" : "jobName") + " in URI";
            logger.warn(String.format("error from session %d: no %s specified in uri (%s)", sessionContext.getId(),
                    (isJobId ? "jobId" : "jobName"), sessionContext.getUri()));
            try {
                sess.getRemote().sendString(error);
            } catch (IOException e) {
                logger.warn("Couldn't send error message to client (session id: " + sessionContext.getId() + "): " + error);
            }
            sess.close();
            return;
        }
        try {
            setSinkParameters();
        } catch (UnsupportedEncodingException e) {
            try {
                logger.error("Can't get sink parameters: " + e.getMessage(), e);
                sess.getRemote().sendString("error: can't create sink parameters: " + e.getMessage());
            } catch (IOException e1) {
                logger.error(String.format("Error sending error message (%s) to client: %s", e.getMessage(), e1.getMessage()), e1);
            }
            sess.close();
        }
        Func0<Observable<Observable<MantisServerSentEvent>>> localResultsGetter =
                () -> JobSinkConnector.getResults(isJobId, mantisClient, target, sinkParameters);
        Func1<String, Observable<MantisServerSentEvent>> remoteResultsGetter = region -> remoteSinkConnector.getResults(
                region, isJobId ? JobConnectById.handlerName : JobConnectByName.handlerName,
                target, sinkParameters
        );
        ServletConx conx = WebsocketUtils.getWSConx(sess);

        subscription = process(sendTunnelPings, conx, sessionContext, stats, tags, target, localResultsGetter, remoteResultsGetter,
                s -> onWebSocketClose(-1, s), closed, registry, propertyRepository, workerThreadPool);
    }


    public static Subscription process(boolean sendTnlPings, ServletConx connection, SessionContext sessionContext, Stats stats,
                                       List<Tag> tags, String target, Func0<Observable<Observable<MantisServerSentEvent>>> localResultsGetter,
                                       Func1<String, Observable<MantisServerSentEvent>> remoteResultsGetter,
                                       Action1<String> onError, AtomicBoolean closeOut, Registry registry, PropertyRepository propertyRepository, WorkerThreadPool workerThreadPool) {

        final List<String> regions = sessionContext.getRegions();

        final PublishSubject<Integer> untilSubject = PublishSubject.create();
        Observable<Observable<String>> results =
                JobSinkConnector.composeCrossRegionObservables(regions, sendTnlPings, untilSubject, localResultsGetter,
                        remoteResultsGetter, tags, sessionContext.getId());
        return intlWriteResults(
                connection, sessionContext, stats, tags, target, results, onError, closeOut, untilSubject, registry, propertyRepository, workerThreadPool);
    }

    private static Subscription intlWriteResults(ServletConx connection, SessionContext sessionContext, Stats stats,
                                                 List<Tag> tags, String target, Observable<Observable<String>> results,
                                                 Action1<String> onError, AtomicBoolean closeOut,
                                                 Observable<Integer> untilSubject,
                                                 Registry registry,
                                                 PropertyRepository propertyRepository,
                                                 WorkerThreadPool workerThreadPool) {

        Property<Integer> CAPACITY = propertyRepository.get("mantisapi.stream.buffer.size", Integer.class).orElse(1000);

        final BlockingQueue<String> queue = new LinkedBlockingQueue<>(CAPACITY.get());
        final AtomicReference<Exception> errorRef = new AtomicReference<>();
        final ScheduledFuture<?> writerFuture = startWriter(connection, queue, tags, errorRef, stats, registry, workerThreadPool);
        final PublishSubject<Boolean> endRequestedSubject = PublishSubject.create();

        Counter numDroppedMessagesCounter = SpectatorUtils.buildAndRegisterCounter(registry, numDroppedMessagesCounterName, tags);
        Counter numDroppedBytesCounter = SpectatorUtils.buildAndRegisterCounter(registry, numDroppedBytesCounterName, tags);

        sessionContext.setEndSessionListener(() -> endRequestedSubject.onNext(true));
        return results
                .doOnError(throwable -> {
                    logger.error(throwable.getMessage(), throwable);
                    try {
                        connection.sendMessage("error getting data: " + throwable.getMessage());
                    } catch (IOException e) {
                        logger.warn("Couldn't send error message to client (session id " + sessionContext.getId() + "): " + throwable.getMessage());
                    }
                    onError.call(throwable.getMessage());
                    connection.close();
                })
                .mergeWith(
                        Observable.timer(5, 5, TimeUnit.SECONDS)
                                .map(aLong -> Observable.just(DUMMY_TIMER_DATA))
                )
                .takeWhile(o -> connection.isOpen())
                .onErrorResumeNext(throwable -> Observable.<Observable<String>>empty())
                .flatMap(eventObservable -> eventObservable
                        .takeWhile(mantisServerSentEvents -> !closeOut.get())
                        .buffer(flushIntervalMillis, TimeUnit.MILLISECONDS)
                        .filter(s -> s != null && !s.isEmpty())
                        .flatMap(list -> {
                            final Exception e = errorRef.get();
                            if (e != null) {
                                logger.error("Error sending message to client for jobconnect " +
                                        target + ": " + e.getMessage(), e);
                                return Observable.error(e);
                            }
                            for (String s : list) {
                                if (!queue.offer(s) && !DUMMY_TIMER_DATA.equals(s)) {
                                    numDroppedMessagesCounter.increment();
                                    numDroppedBytesCounter.increment(s.length());
                                }
                            }
                            return Observable.empty();
                        })
                )
                .takeUntil(untilSubject)
                .takeUntil(endRequestedSubject)
                .doOnCompleted(() -> tearDown(sessionContext, writerFuture, queue, connection))
                .doOnUnsubscribe(() -> tearDown(sessionContext, writerFuture, queue, connection))
                .subscribe();
    }

    private static void tearDown(SessionContext sessionContext, ScheduledFuture<?> writerFuture, BlockingQueue<String> queue, ServletConx connection) {
        writerFuture.cancel(true);
        queue.clear();
        if (sessionContext != null)
            sessionContext.endSession();
        connection.close();
    }

    private static ScheduledFuture<?> startWriter(final ServletConx connection, final BlockingQueue<String> queue,
                                                  final List<Tag> tags, final AtomicReference<Exception> errorRef, Stats stats, Registry registry,
                                                  WorkerThreadPool workerThreadPool) {
        return getScheduledFuture(connection, queue, tags, errorRef, stats, wsQueueSizeGaugeName, DUMMY_TIMER_DATA, numMessagesCounterName, numBytesCounterName, flushIntervalMillis, registry, workerThreadPool);
    }

    public static ScheduledFuture<?> getScheduledFuture(ServletConx connection, BlockingQueue<String> queue, List<Tag> tags, AtomicReference<Exception> errorRef, Stats stats, String wsQueueSizeGaugeName, String dummyTimerData, String numMessagesCounterName, String numBytesCounterName, long flushIntervalMillis, Registry registry, ScheduledThreadPoolExecutor threadPool) {

        Counter numMessagesCounter = SpectatorUtils.buildAndRegisterCounter(registry, numMessagesCounterName, tags);
        Counter numBytesCounter = SpectatorUtils.buildAndRegisterCounter(registry, numBytesCounterName, tags);
        AtomicDouble websocketQueueSizeGauge = SpectatorUtils.buildAndRegisterGauge(registry, wsQueueSizeGaugeName, tags);

        return threadPool.scheduleWithFixedDelay(() -> {
            if (!connection.isOpen())
                return;
            final List<String> items = new LinkedList<>();
            queue.drainTo(items);
            if (items.size() > 0) {
                websocketQueueSizeGauge.set(items.size());
                long nMsgs = 0L;
                long nBytes = 0L;
                for (String s : items) {
                    if (dummyTimerData.equals(s))
                        continue;
                    // update stats once per 10 (arbitrary number) messages instead of waiting till the end
                    if (!connection.isOpen() || nMsgs % 10 == 0) {
                        if (nMsgs > 0) {
                            stats.incrementNumMessages(nMsgs);
                            stats.incrementNumBytes(nBytes);
                            numMessagesCounter.increment(nMsgs);
                            numBytesCounter.increment(nBytes);
                            nMsgs = 0;
                            nBytes = 0;
                        }
                        if (!connection.isOpen())
                            return;
                    }
                    try {
                        connection.sendMessage(s);
                        nMsgs++;
                        nBytes += s.length();
                    } catch (IOException e) {
                        errorRef.set(e);
                        // it is possible we got this error after sending a few of the messages, finish updating the stats
                        if (nMsgs > 0) {
                            stats.incrementNumMessages(nMsgs);
                            stats.incrementNumBytes(nBytes);
                            numMessagesCounter.increment(nMsgs);
                            numBytesCounter.increment(nBytes);
                            nMsgs = 0;
                            nBytes = 0;
                        }
                        break;
                    }
                }
                // update stats for any messages written since last update
                if (nMsgs > 0) {
                    stats.incrementNumMessages(nMsgs);
                    stats.incrementNumBytes(nBytes);
                    numMessagesCounter.increment(nMsgs);
                    numBytesCounter.increment(nBytes);
                    nMsgs = 0;
                    nBytes = 0;
                }
                items.clear();
            }
        }, flushIntervalMillis, flushIntervalMillis, TimeUnit.MILLISECONDS);
    }


}
