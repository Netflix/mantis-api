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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.ServletConx;
import io.mantisrx.api.handlers.domain.MantisEventSource;
import io.mantisrx.api.handlers.servlets.JobConnectById;
import io.mantisrx.api.handlers.servlets.JobConnectByIdWebSocketServlet;
import io.mantisrx.api.handlers.servlets.JobConnectByName;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.MantisClientUtil;
import io.mantisrx.api.handlers.utils.QueryParams;
import io.mantisrx.api.handlers.ws.JobConnectWebSocket;
import io.mantisrx.api.metrics.Stats;
import io.mantisrx.client.MantisClient;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SinkParameters;
import org.eclipse.jetty.servlets.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func0;
import rx.functions.Func1;


public class JobSinkServletConnector {

    private static Logger logger = LoggerFactory.getLogger(JobSinkServletConnector.class);
    private static final String TWO_NEWLINES = "\n\n";
    private static final String SSE_DATA_PREFIX = "data: ";
    private final MantisClient mantisClient;
    private final boolean isJobId;
    private final Stats stats;
    private final List<Tag> tags;
    private final Map<String, List<String>> qryParams;
    private final SessionContext sessionContext;
    private final RemoteSinkConnector remoteSinkConnector;
    private final boolean sendTunnelPings;
    private final AtomicBoolean hasErrored = new AtomicBoolean();
    private final CountDownLatch latch = new CountDownLatch(1);

    private transient final PropertyRepository propertyRepository;
    private transient final Property<Boolean> sourceSamplingEnabled;
    private transient final WorkerThreadPool workerThreadPool;
    private transient final Registry registry;

    private final Counter endpointCounter;

    public JobSinkServletConnector(MantisClient mantisClient, boolean isJobId, Stats stats,
                                   Map<String, List<String>> qryParams, SessionContext httpSessionCtx,
                                   RemoteSinkConnector remoteSinkConnector,
                                   PropertyRepository propertyRepository,
                                   Registry registry,
                                   WorkerThreadPool workerThreadPool) {
        this.mantisClient = mantisClient;
        this.isJobId = isJobId;
        this.stats = stats;
        this.qryParams = qryParams;
        this.sessionContext = httpSessionCtx;
        this.remoteSinkConnector = remoteSinkConnector;
        sendTunnelPings = qryParams != null && qryParams.get(QueryParams.TunnelPingParamName) != null &&
                Boolean.valueOf(qryParams.get(QueryParams.TunnelPingParamName).get(0));
        tags = QueryParams.getTaglist(qryParams, sessionContext.getId(), sessionContext.getUri());

        this.propertyRepository = propertyRepository;
        this.registry = registry;
        this.workerThreadPool = workerThreadPool;
        this.sourceSamplingEnabled = propertyRepository.get(PropertyNames.sourceSamplingEnabled, Boolean.class).orElse(false);
        endpointCounter = SpectatorUtils.buildAndRegisterCounter(registry, JobConnectByIdWebSocketServlet.endpointName);
    }

    public void connect(String target, EventSource.Emitter emitter) {
        endpointCounter.increment();
        if (target == null || target.isEmpty()) {
            String error = "Must provide " + (isJobId ? "jobId" : "jobName") + " in URI";
            logger.warn(String.format("error from session %d: no %s specified in uri (%s)", this.sessionContext.getId(),
                    (isJobId ? "jobId" : "jobName"), this.sessionContext.getUri()));
            try {
                emitter.data(error);
            } catch (IOException e) {
                logger.warn("Couldn't send error message to client (session id: " + this.sessionContext.getId() + "): " + error);
            }
            return;
        }
        boolean isSourceJob = isJobId ?
                MantisClientUtil.isSourceJobId(target) :
                MantisClientUtil.isSourceJobName(target);
        final SinkParameters sinkParameters;
        try {

            sinkParameters = JobSinkConnector.getSinkParameters(qryParams, isSourceJob, sourceSamplingEnabled);
        } catch (UnsupportedEncodingException e) {
            try {
                String error = String.format("Error in sink connect request's queryParams [%s], error: %s",
                        qryParams, e.getMessage());
                emitter.data(error);
            } catch (IOException e1) {
                logger.warn("Couldn't send error message to client (session id: " + this.sessionContext.getId() +
                        "): error in sink connect request's queryParams " + qryParams + ", error: " + e.getMessage());
            }
            return;
        }

        Func0<Observable<Observable<MantisServerSentEvent>>> localResultsGetter =
                () -> JobSinkConnector.getResults(isJobId, mantisClient, target, sinkParameters);
        Func1<String, Observable<MantisServerSentEvent>> remoteResultsGetter = region -> remoteSinkConnector.getResults(
                region, isJobId ? JobConnectById.handlerName : JobConnectByName.handlerName,
                target, sinkParameters
        );

        ServletConx conx = getServletConx(emitter, hasErrored, latch, this.sessionContext);
        JobConnectWebSocket.process(sendTunnelPings, conx, this.sessionContext, stats, tags, target,
                localResultsGetter, remoteResultsGetter,
                s -> {
                    hasErrored.set(true);
                    latch.countDown();
                },
                hasErrored,
                registry,
                propertyRepository,
                workerThreadPool);
    }

    public void connect(String target, HttpServletResponse response) {
        endpointCounter.increment();
        if (target == null || target.isEmpty()) {
            String error = "Must provide " + (isJobId ? "jobId" : "jobName") + " in URI";
            logger.warn(String.format("error from session %d: no %s specified in uri (%s)", this.sessionContext.getId(),
                    (isJobId ? "jobId" : "jobName"), this.sessionContext.getUri()));
            try {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().print(error);
            } catch (IOException e) {
                logger.warn("Couldn't send error message to client (session id: " + this.sessionContext.getId() + "): " + error);
            }
            return;
        }
        boolean isSourceJob = isJobId ?
                MantisClientUtil.isSourceJobId(target) :
                MantisClientUtil.isSourceJobName(target);
        final SinkParameters sinkParameters;
        try {
            sinkParameters = JobSinkConnector.getSinkParameters(qryParams, isSourceJob, sourceSamplingEnabled);
        } catch (UnsupportedEncodingException e) {
            try {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().print(String.format("Error in sink connect request's queryParams [%s], error: %s",
                        qryParams, e.getMessage()));
            } catch (IOException e1) {
                logger.warn("Couldn't send error message to client (session id: " + this.sessionContext.getId() +
                        "): error in sink connect request's queryParams " + qryParams + ", error: " + e.getMessage());
            }
            return;
        }

        response.setStatus(HttpServletResponse.SC_OK);
        HttpUtils.addBaseHeaders(response, "GET");
        HttpUtils.addSseHeaders(response);

        Func0<Observable<Observable<MantisServerSentEvent>>> localResultsGetter =
                () -> JobSinkConnector.getResults(isJobId, mantisClient, target, sinkParameters);

        Func1<String, Observable<MantisServerSentEvent>> remoteResultsGetter = region -> remoteSinkConnector.getResults(
                region, isJobId ? JobConnectById.handlerName : JobConnectByName.handlerName,
                target, sinkParameters
        );
        ServletConx conx = getServletConx(response, hasErrored, latch, this.sessionContext);
        final Subscription subscription = JobConnectWebSocket.process(sendTunnelPings, conx, this.sessionContext, stats, tags, target,
                localResultsGetter, remoteResultsGetter,
                s -> {
                    hasErrored.set(true);
                    latch.countDown();
                },
                hasErrored,
                registry,
                propertyRepository,
                workerThreadPool);
        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.warn("Will retry after getting interrupted waiting for latch: " + e.getMessage());
            }
        }
        if (!subscription.isUnsubscribed())
            subscription.unsubscribe();
        this.sessionContext.endSession();
    }

    public void connect(String target, ServletResponse response) {
        endpointCounter.increment();
        if (target == null || target.isEmpty()) {
            String error = "Must provide " + (isJobId ? "jobId" : "jobName") + " in URI";
            logger.warn(String.format("error from session %d: no %s specified in uri (%s)", this.sessionContext.getId(),
                    (isJobId ? "jobId" : "jobName"), this.sessionContext.getUri()));
            try {
                response.getWriter().print(error);
            } catch (IOException e) {
                logger.warn("Couldn't send error message to client (session id: " + this.sessionContext.getId() + "): " + error);
            }
            return;
        }
        boolean isSourceJob = isJobId ?
                MantisClientUtil.isSourceJobId(target) :
                MantisClientUtil.isSourceJobName(target);
        final SinkParameters sinkParameters;
        try {
            sinkParameters = JobSinkConnector.getSinkParameters(qryParams, isSourceJob, sourceSamplingEnabled);
        } catch (UnsupportedEncodingException e) {
            try {
                response.getWriter().print(String.format("Error in sink connect request's queryParams [%s], error: %s",
                        qryParams, e.getMessage()));
            } catch (IOException e1) {
                logger.warn("Couldn't send error message to client (session id: " + this.sessionContext.getId() +
                        "): error in sink connect request's queryParams " + qryParams + ", error: " + e.getMessage());
            }
            return;
        }

        Func0<Observable<Observable<MantisServerSentEvent>>> localResultsGetter =
                () -> JobSinkConnector.getResults(isJobId, mantisClient, target, sinkParameters);

        Func1<String, Observable<MantisServerSentEvent>> remoteResultsGetter = region -> remoteSinkConnector.getResults(
                region, isJobId ? JobConnectById.handlerName : JobConnectByName.handlerName,
                target, sinkParameters
        );
        ServletConx conx = getServletConx(response, hasErrored, latch, this.sessionContext);
        final Subscription subscription = JobConnectWebSocket.process(sendTunnelPings, conx, this.sessionContext, stats, tags, target,
                localResultsGetter, remoteResultsGetter,
                s -> {
                    hasErrored.set(true);
                    latch.countDown();
                },
                hasErrored,
                registry,
                propertyRepository,
                workerThreadPool);
        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.warn("Will retry after getting interrupted waiting for latch: " + e.getMessage());
            }
        }
        if (!subscription.isUnsubscribed())
            subscription.unsubscribe();
        this.sessionContext.endSession();
    }

    public static ServletConx getServletConx(final ServletResponse response,
                                             AtomicBoolean hasErrored, CountDownLatch latch, SessionContext sessionContext) {
        return new ServletConx() {
            @Override
            public void sendMessage(String s) throws IOException {
                //response.setCharacterEncoding("UTF-8");
                if (!hasErrored.get()) {
                    try {
                        response.getWriter().print(SSE_DATA_PREFIX + s + TWO_NEWLINES);
                        response.getWriter().flush();
                        if (response.getWriter().checkError()) {
                            throw new IOException("Writer destination has error");
                        }
                        //                        response.getOutputStream().print(SSE_DATA_PREFIX + s + TWO_NEWLINES);
                        //                        response.flushBuffer();
                    } catch (IOException e) {
                        logger.info("Session " + sessionContext.getId() + ": error sending message: " + e.getMessage());
                        hasErrored.set(true);
                    }
                }
            }

            @Override
            public void close() {
                latch.countDown();
            }

            @Override
            public boolean isOpen() {
                return !hasErrored.get();
            }
        };
    }

    public static ServletConx getServletConx(final HttpServletResponse response,
                                             AtomicBoolean hasErrored, CountDownLatch latch, SessionContext sessionContext) {
        return new ServletConx() {
            @Override
            public void sendMessage(String s) throws IOException {
                //response.setCharacterEncoding("UTF-8");
                if (!hasErrored.get()) {
                    try {
                        response.getWriter().print(SSE_DATA_PREFIX + s + TWO_NEWLINES);
                        response.getWriter().flush();
                        if (response.getWriter().checkError()) {
                            throw new IOException("Writer destination has error");
                        }
                        //                        response.getOutputStream().print(SSE_DATA_PREFIX + s + TWO_NEWLINES);
                        //                        response.flushBuffer();
                    } catch (IOException e) {
                        logger.info("Session " + sessionContext.getId() + ": error sending message: " + e.getMessage());
                        hasErrored.set(true);
                    }
                }
            }

            @Override
            public void close() {
                latch.countDown();
            }

            @Override
            public boolean isOpen() {
                return !hasErrored.get();
            }
        };
    }

    public static ServletConx getServletConx(final MantisEventSource eventSource,
                                             AtomicBoolean hasErrored, CountDownLatch latch, SessionContext sessionContext) {
        return new ServletConx() {
            @Override
            public void sendMessage(String s) throws IOException {
                if (!hasErrored.get()) {
                    try {
                        eventSource.emitEvent(s);
                    } catch (IOException e) {
                        logger.info("Session " + sessionContext.getId() + ": error sending message: " + e.getMessage());
                        hasErrored.set(true);
                        eventSource.onClose();
                    }
                }
            }

            @Override
            public void close() {
                latch.countDown();
            }

            @Override
            public boolean isOpen() {
                return !hasErrored.get();
            }
        };
    }

    public static ServletConx getServletConx(final EventSource.Emitter eventSource,
                                             AtomicBoolean hasErrored, CountDownLatch latch, SessionContext sessionContext) {
        return new ServletConx() {
            @Override
            public void sendMessage(String s) throws IOException {
                if (!hasErrored.get()) {
                    try {
                        eventSource.data(s);
                    } catch (IOException e) {
                        logger.info("Session " + sessionContext.getId() + ": error sending message: " + e.getMessage());
                        hasErrored.set(true);
                        eventSource.close();
                    }
                }
            }

            @Override
            public void close() {
                latch.countDown();
            }

            @Override
            public boolean isOpen() {
                return !hasErrored.get();
            }
        };
    }
}
