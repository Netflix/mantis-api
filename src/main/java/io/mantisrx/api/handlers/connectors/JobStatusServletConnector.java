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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.ServletConx;
import io.mantisrx.api.handlers.servlets.JobStatusWebSocketServlet;
import io.mantisrx.api.handlers.servlets.SSEWebSocketServletBase;
import io.mantisrx.api.handlers.utils.QueryParams;
import io.mantisrx.api.handlers.ws.JobStatusWebSocket;
import io.mantisrx.api.metrics.Stats;
import io.mantisrx.client.MantisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func0;


public class JobStatusServletConnector {

    private static Logger logger = LoggerFactory.getLogger(JobStatusServletConnector.class);
    private static final String TWO_NEWLINES = "\n\n";
    private static final String SSE_DATA_PREFIX = "data: ";
    private final MantisClient mantisClient;
    private final boolean isJobId;
    private final Stats stats;
    private final List<Tag> tags;
    private final Map<String, List<String>> qryParams;
    private final SessionContext sessionContext;

    private final boolean sendTunnelPings;
    private final AtomicBoolean hasErrored = new AtomicBoolean();
    private final CountDownLatch latch = new CountDownLatch(1);

    Counter endpointCounter;
    private transient final Registry registry;
    private transient final WorkerThreadPool workerThreadPool;

    public JobStatusServletConnector(MantisClient mantisClient, boolean isJobId, Stats stats,
                                     Map<String, List<String>> qryParams, SessionContext httpSessionCtx, Registry registry, WorkerThreadPool workerThreadPool) {
        this.mantisClient = mantisClient;
        this.isJobId = isJobId;
        this.stats = stats;
        this.qryParams = qryParams;
        this.sessionContext = httpSessionCtx;
        sendTunnelPings = qryParams != null && qryParams.get(QueryParams.TunnelPingParamName) != null &&
                Boolean.valueOf(qryParams.get(QueryParams.TunnelPingParamName).get(0));
        tags = QueryParams.getTaglist(qryParams, sessionContext.getId(), sessionContext.getUri());

        endpointCounter = SpectatorUtils.buildAndRegisterCounter(registry, JobStatusWebSocketServlet.endpointName);
        this.registry = registry;
        this.workerThreadPool = workerThreadPool;
    }

    public void connect(String target, SSEWebSocketServletBase.EventSourceEmitter emitter) {
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

        Func0<Observable<Observable<String>>> localResultsGetter = () -> Observable.just(mantisClient.getJobStatusObservable(target));
        ServletConx conx = getServletConx(emitter, hasErrored, latch, this.sessionContext);

        JobStatusWebSocket.process(sendTunnelPings, conx, this.sessionContext, stats, tags, target,
                localResultsGetter,
                s -> {
                    hasErrored.set(true);
                    latch.countDown();
                },
                hasErrored,
                registry,
                workerThreadPool);
    }

    public static ServletConx getServletConx(final SSEWebSocketServletBase.EventSourceEmitter emitter,
                                             AtomicBoolean hasErrored, CountDownLatch latch, SessionContext sessionContext) {
        return new ServletConx() {
            @Override
            public void sendMessage(String s) throws IOException {
                if (!hasErrored.get()) {
                    try {
                        emitter.data(s);
                    } catch (IOException e) {
                        logger.info("Session " + sessionContext.getId() + ": error sending message: " + e.getMessage());
                        hasErrored.set(true);
                        emitter.close();
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
