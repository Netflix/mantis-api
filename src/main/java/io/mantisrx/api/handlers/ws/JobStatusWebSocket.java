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
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.ServletConx;
import io.mantisrx.api.handlers.connectors.RemoteSinkConnector;
import io.mantisrx.api.handlers.utils.QueryParams;
import io.mantisrx.api.metrics.Stats;
import io.mantisrx.client.MantisClient;
import io.mantisrx.runtime.parameter.SinkParameters;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.subjects.PublishSubject;


public class JobStatusWebSocket extends WebSocketAdapter {

    private static Logger logger = LoggerFactory.getLogger(JobStatusWebSocket.class);
    private final boolean isJobId;
    private final MantisClient mantisClient;
    private final Registry registry;
    private String target;
    private final Map<String, List<String>> qryParams;
    private static long flushIntervalMillis = 50;
    private final SessionContext sessionContext;
    private volatile Subscription subscription = null;
    private volatile AtomicBoolean closed = new AtomicBoolean(false);
    private final Stats stats;
    private final List<Tag> tags;
    private static final String DUMMY_TIMER_DATA = "DUMMY_TIMER_DATA";
    private static final String numMessagesCounterName = "numSinkMessages";
    private static final String numBytesCounterName = "numSinkBytes";
    private static final String wsQueueSizeGaugeName = "wsQueueSize";
    private final RemoteSinkConnector remoteSinkConnector;
    private SinkParameters sinkParameters = null;
    private final boolean sendTunnelPings;
    private static final int CAPACITY = 1000;
    private final WorkerThreadPool workerThreadPool;

    private Counter endpointCounter;

    public JobStatusWebSocket(boolean isJobId, MantisClient mantisClient, Stats stats, Map<String, List<String>> qryParams, SessionContext sessionContext, RemoteSinkConnector remoteSinkConnector, Registry registry, WorkerThreadPool workerThreadPool) {
        this.isJobId = isJobId;
        this.mantisClient = mantisClient;
        this.stats = stats;
        this.qryParams = qryParams;
        this.sessionContext = sessionContext;
        this.remoteSinkConnector = remoteSinkConnector;
        sendTunnelPings = qryParams != null && qryParams.get(QueryParams.TunnelPingParamName) != null &&
                Boolean.valueOf(qryParams.get(QueryParams.TunnelPingParamName).get(0));
        tags = QueryParams.getTaglist(qryParams, sessionContext.getId(), sessionContext.getUri());
        this.registry = registry;
        this.workerThreadPool = workerThreadPool;

        endpointCounter = SpectatorUtils.buildAndRegisterCounter(registry, "jobStatus");
    }

    public void setTarget(String target) {
        this.target = target;
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
        endpointCounter.increment();
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
        Func0<Observable<Observable<String>>> localResultsGetter = () -> Observable.just(mantisClient.getJobStatusObservable(target));
        ServletConx conx = WebsocketUtils.getWSConx(sess);

        subscription = process(sendTunnelPings, conx, sessionContext, stats, tags, target, localResultsGetter,
                s -> onWebSocketClose(-1, s), closed, registry, workerThreadPool);

    }

    public static Subscription process(boolean sendTnlPings, ServletConx connection, SessionContext sessionContext, Stats stats,
                                       List<Tag> tags, String target, Func0<Observable<Observable<String>>> localResultsGetter,
                                       //Func1<String, Observable<MantisServerSentEvent>> remoteResultsGetter,
                                       Action1<String> onError, AtomicBoolean closeOut, Registry registry,
                                       WorkerThreadPool workerThreadPool) {

        final PublishSubject<Integer> untilSubject = PublishSubject.create();
        //        Observable<Observable<String>> results =
        //                JobSinkConnector.composeCrossRegionObservables(regions, sendTnlPings, untilSubject, localResultsGetter,
        //                        remoteResultsGetter, tags, sessionContext.getId());
        Observable<Observable<String>> results = localResultsGetter.call();

        return intlWriteResults(
                connection, sessionContext, stats, tags, target, results, onError, closeOut, untilSubject, registry, workerThreadPool);
    }

    private static Subscription intlWriteResults(ServletConx connection, SessionContext sessionContext, Stats stats,
                                                 List<Tag> tags, String target, Observable<Observable<String>> results,
                                                 Action1<String> onError, AtomicBoolean closeOut,
                                                 Observable<Integer> untilSubject, Registry registry,
                                                 WorkerThreadPool workerThreadPool) {
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>(CAPACITY);
        final AtomicReference<Exception> errorRef = new AtomicReference<>();

        Counter numDroppedMessagesCounter = SpectatorUtils.buildAndRegisterCounter(registry, "numDroppedSinkMessages", tags);
        Counter numDroppedBytesCounter = SpectatorUtils.buildAndRegisterCounter(registry, "numDroppedSinkBytes", tags);

        final ScheduledFuture<?> writerFuture = startWriter(connection, queue, tags, errorRef, stats, registry, workerThreadPool);
        final PublishSubject<Boolean> endRequestedSubject = PublishSubject.create();
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
                        Observable.interval(5, 5, TimeUnit.SECONDS)
                                .map(aLong -> Observable.just(DUMMY_TIMER_DATA))
                )
                .takeWhile(o -> connection.isOpen())
                .onErrorResumeNext(throwable -> Observable.<Observable<String>>empty())
                .flatMap(eventObservable -> eventObservable
                        .map((e) -> {
                            if (e != null && !e.isEmpty() && e.startsWith("{\"error\":")) {
                                Exception ex = new Exception(e);
                                try {
                                    connection.sendMessage("error getting data: " + ex.getMessage());
                                } catch (IOException ioE) {
                                    logger.warn("Couldn't send error message to client (session id " + sessionContext.getId() + "): " + ioE.getMessage());
                                }
                                onError.call(ex.getMessage());
                                connection.close();
                            }
                            return e;
                        })
                        .takeWhile(mantisServerSentEvents -> !closeOut.get())
                        .buffer(flushIntervalMillis, TimeUnit.MILLISECONDS)
                        .filter(s -> s != null && !s.isEmpty())
                        .flatMap(list -> {
                            final Exception e = errorRef.get();
                            if (e != null) {
                                logger.error("Error sending message to client for jobstatus " +
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
                .doOnError(__ -> tearDown(sessionContext, writerFuture, queue, connection))
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
                                                  final List<Tag> tags, final AtomicReference<Exception> errorRef, Stats stats, Registry registry, WorkerThreadPool workerThreadPool) {
        return JobConnectWebSocket.getScheduledFuture(connection, queue, tags, errorRef, stats, wsQueueSizeGaugeName, DUMMY_TIMER_DATA, numMessagesCounterName, numBytesCounterName, flushIntervalMillis, registry, workerThreadPool);
    }


}
