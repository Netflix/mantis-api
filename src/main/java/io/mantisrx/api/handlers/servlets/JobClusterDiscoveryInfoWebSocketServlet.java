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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.domain.MantisEventSource;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.JobDiscoveryInfoManager;
import io.mantisrx.api.handlers.utils.JobDiscoveryLookupKey;
import io.mantisrx.api.handlers.utils.PathUtils;
import io.mantisrx.api.handlers.utils.RetryUtils;
import io.mantisrx.client.MantisClient;
import org.eclipse.jetty.servlets.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func1;


/**
 * curl "http://<host>:<port>/jobClusters/discoveryInfoStream/<jobCluster>"
 */
public class JobClusterDiscoveryInfoWebSocketServlet extends SSEWebSocketServletBase {

    private static final long serialVersionUID = JobClusterDiscoveryInfoWebSocketServlet.class.hashCode();

    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory.getLogger(JobClusterDiscoveryInfoWebSocketServlet.class);
    private transient final MantisClient mantisClient;
    public static final String endpointName = "jobClusters/discoveryInfoStream";
    public static final String helpMsg = endpointName + "/<JobCluster>";
    private static final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic =
            RetryUtils.getRetryFunc(logger, 5);
    private static final String TWO_NEWLINES = "\n\n";
    private static final String SSE_DATA_PREFIX = "data: ";
    private final ObjectMapper mapper = new ObjectMapper();
    private transient final Registry registry;
    private transient final WorkerThreadPool workerThreadPool;
    private transient final PropertyRepository propertyRepository;


    public JobClusterDiscoveryInfoWebSocketServlet(MantisClient mantisClient, PropertyRepository propertyRepository, Registry registry, WorkerThreadPool workerThreadPool) {
        super(propertyRepository);
        this.mantisClient = mantisClient;
        this.registry = registry;
        this.workerThreadPool = workerThreadPool;
        this.propertyRepository = propertyRepository;
    }


    @Override
    public void doOptions(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    private Observable<String> getPingObservable() {
        return Observable.interval(30, TimeUnit.SECONDS).map((l) -> "ping");
    }

    @Override
    protected EventSource newEventSource(final HttpServletRequest request, final HttpServletResponse response) {
        MantisEventSource mantisEventSource = new MantisEventSource();

        final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
        final SessionContext httpSessionCtx = contextBuilder.createHttpSessionCtx(request.getRemoteAddr(),
                request.getRequestURI() + "?" + request.getQueryString(), request.getMethod());

        String jobCluster = PathUtils.getTokenAfter(request.getPathInfo(), "");
        if (jobCluster == null || jobCluster.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            try {
                response.getOutputStream().print("Job cluster not specified");
                response.flushBuffer();
            } catch (Exception e) {
                logger.warn("Couldn't write message (\"Job cluster not specified\") to client session {}",
                        httpSessionCtx.getId(), e);
            }
            httpSessionCtx.endSession();
            return null;
        }

        Observable<String> schedulingObs = JobDiscoveryInfoManager.getInstance(mantisClient, registry)
                .jobDiscoveryInfoStream(new JobDiscoveryLookupKey(JobDiscoveryLookupKey.LookupType.JOB_CLUSTER, jobCluster))
                .doOnError((t) -> {
                    logger.warn("Timed out relaying request for session id " + httpSessionCtx.getId() +
                            " url=" + request.getRequestURI());
                    try {
                        mantisEventSource.emitEvent(t.getMessage());
                    } catch (IOException e) {
                        logger.error("Couldn't write message ({}) to client session {}", t.getMessage(), httpSessionCtx.getId(), e);
                        httpSessionCtx.endSession();
                    }
                })
                .map((jSchedInfo) -> {
                    try {
                        return mapper.writeValueAsString(jSchedInfo);
                    } catch (Exception e) {
                        return "{\"error\" : " + e.getMessage() + "\"}";
                    }

                });

        Subscription subscription = schedulingObs.mergeWith(getPingObservable())
                .doOnNext(result -> {
                    try {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Sending --> " + result);
                        }
                        mantisEventSource.emitEvent(result);
                    } catch (Exception e) {
                        logger.error("Error writing to client session {}", httpSessionCtx.getId(), e);
                        httpSessionCtx.endSession();
                    }
                })
                .doOnTerminate(httpSessionCtx::endSession)
                .subscribe();

        httpSessionCtx.setEndSessionListener(() -> {
            logger.debug("session {} ended for job cluster discovery stream of {}", httpSessionCtx.getId(), jobCluster);
            if (subscription != null && !subscription.isUnsubscribed()) {
                subscription.unsubscribe();
            }
        });
        return mantisEventSource;
    }
}
