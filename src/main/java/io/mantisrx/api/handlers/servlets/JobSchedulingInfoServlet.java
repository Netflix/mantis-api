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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.JobSchedulingInfoManager;
import io.mantisrx.api.handlers.utils.PathUtils;
import io.mantisrx.api.handlers.utils.RetryUtils;
import io.mantisrx.client.MantisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func1;


/**
 * curl "http://<host>:<port>/api/v1/jobs/schedulingInfo/<jobid>"
 */
public class JobSchedulingInfoServlet extends HttpServlet {

    private static final long serialVersionUID = JobSchedulingInfoServlet.class.hashCode();

    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory.getLogger(JobSchedulingInfoServlet.class);
    private transient final MantisClient mantisClient;
    public static final String endpointName = "schedulingInfo";
    public static final String helpMsg = endpointName + "/<JobId>";
    private static final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic =
            RetryUtils.getRetryFunc(logger, 5);
    private static final String TWO_NEWLINES = "\n\n";
    private static final String SSE_DATA_PREFIX = "data: ";
    private final ObjectMapper mapper = new ObjectMapper();
    private transient final Registry registry;
    private transient final WorkerThreadPool workerThreadPool;
    private transient final PropertyRepository propertyRepository;


    public JobSchedulingInfoServlet(MantisClient mantisClient, PropertyRepository propertyRepository, Registry registry, WorkerThreadPool workerThreadPool) {
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
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
        final SessionContext httpSessionCtx = contextBuilder.createHttpSessionCtx(request.getRemoteAddr(),
                request.getRequestURI() + "?" + request.getQueryString(), request.getMethod());
        CountDownLatch latch = new CountDownLatch(1);
        Subscription subscription = null;
        try {
            String uri = request.getRequestURI();
            if (request.getQueryString() != null && !request.getQueryString().isEmpty())
                uri = uri + "?" + request.getQueryString();
            final String completeUri = uri;

            String jobId = PathUtils.getTokenAfter(request.getPathInfo(), "");
            if (jobId == null || jobId.isEmpty()) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                try {

                    response.getOutputStream().print("JobId not specified");
                    response.flushBuffer();
                } catch (IOException e) {
                    logger.warn("Couldn't write message (" + e.getMessage() + ") to client session " +
                            httpSessionCtx.getId() + ": " + e.getMessage());
                }
                latch.countDown();
            } else {
                Observable<String> schedulingObs = JobSchedulingInfoManager.getInstance(mantisClient, registry).getSchedulingInfoObservable(jobId)
                        .doOnError((t) -> {
                            logger.warn("Timed out relaying request for session id " + httpSessionCtx.getId() +
                                    " url=" + request.getRequestURI());
                            response.setStatus(HttpServletResponse.SC_REQUEST_TIMEOUT);
                            try {

                                response.getOutputStream().print(t.getMessage());
                                response.flushBuffer();
                            } catch (IOException e) {
                                logger.error("Couldn't write message (" + t.getMessage() + ") to client session " +
                                        httpSessionCtx.getId() + ": " + e.getMessage());
                            }
                            latch.countDown();
                        })
                        .map((jSchedInfo) -> {
                            try {
                                return mapper.writeValueAsString(jSchedInfo);
                            } catch (Exception e) {
                                return "{\"error\" : " + e.getMessage() + "\"}";
                            }

                        });

                subscription =
                        schedulingObs.mergeWith(getPingObservable())
                                .doOnNext(result -> {

                                    HttpUtils.addBaseHeaders(response, "GET");
                                    HttpUtils.addSseHeaders(response);
                                    try {
                                        if (logger.isDebugEnabled()) { logger.info("Sending --> " + result); }

                                        response.getWriter().print(SSE_DATA_PREFIX + result + TWO_NEWLINES);
                                        response.getWriter().flush();
                                        if (response.getWriter().checkError()) {
                                            throw new Exception("Error writing to client ");
                                        }
                                    } catch (Exception e) {
                                        logger.error("Error writing to client session " + httpSessionCtx.getId() + ": " + e.getMessage());
                                        latch.countDown();
                                    }
                                })
                                .doOnTerminate(latch::countDown)

                                .subscribe();

                while (latch.getCount() > 0) {
                    try {
                        latch.await();
                        if (subscription != null && !subscription.isUnsubscribed()) {
                            subscription.unsubscribe();
                        }
                    } catch (InterruptedException e) {
                        logger.warn("Will retry after getting interrupted waiting for latch: " + e.getMessage());
                    }
                }

            }

        } finally {
            if (subscription != null && !subscription.isUnsubscribed())
                subscription.unsubscribe();
            httpSessionCtx.endSession();
        }
    }
}
