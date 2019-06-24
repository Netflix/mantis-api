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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.JobSubmitAndConnect;
import io.mantisrx.api.handlers.ServletConx;
import io.mantisrx.api.handlers.connectors.JobSinkConnector;
import io.mantisrx.api.handlers.connectors.JobSinkServletConnector;
import io.mantisrx.api.handlers.connectors.RemoteSinkConnector;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.MantisClientUtil;
import io.mantisrx.api.handlers.utils.QueryParams;
import io.mantisrx.api.handlers.ws.JobConnectWebSocket;
import io.mantisrx.api.handlers.ws.JobSubmitAndConnectWebSocket;
import io.mantisrx.client.MantisClient;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.mantisrx.server.master.client.MasterClientWrapper;
import org.eclipse.jetty.servlets.EventSource;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;


public class JobSubmitAndConnectServlet extends SSEWebSocketServletBase {

    private static final long serialVersionUID = JobSubmitAndConnectServlet.class.hashCode();
    @SuppressWarnings("unused")
    private static Logger logger = LoggerFactory.getLogger(JobSubmitAndConnectServlet.class);
    private transient final MantisClient mantisClient;
    private transient final MasterClientWrapper masterClientWrapper;
    private transient final RemoteSinkConnector remoteSinkConnector;
    public static final String endpointName = "jobsubmitandconnect";
    public static final String helpMsg = "(POST) " + endpointName + " (body: refer to docs)";
    private transient final Registry registry;
    private transient final PropertyRepository propertyRepository;
    private transient final WorkerThreadPool workerThreadPool;

    public JobSubmitAndConnectServlet(MantisClient mantisClient, MasterClientWrapper masterClientWrapper, RemoteSinkConnector remoteSinkConnector, Registry registry, PropertyRepository propertyRepository, WorkerThreadPool workerThreadPool) {
        super(propertyRepository);
        this.mantisClient = mantisClient;
        this.masterClientWrapper = masterClientWrapper;
        this.remoteSinkConnector = remoteSinkConnector;
        this.registry = registry;
        this.workerThreadPool = workerThreadPool;
        this.propertyRepository = propertyRepository;
    }


    @Override
    public void configure(WebSocketServletFactory factory) {
        super.configure(factory);
        factory.setCreator((ServletUpgradeRequest req, ServletUpgradeResponse resp) -> {
            final Map<String, List<String>> qryParams = HttpUtils.getQryParams(req.getQueryString());
            final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
            final SessionContext webSocketSessionCtx = contextBuilder.createWebSocketSessionCtx(req.getRemoteAddress(),
                    req.getRequestURI() + "?" + req.getQueryString());
            return createJobSubmitAndConnectWebSocket(webSocketSessionCtx, qryParams);
        });
    }

    public WebSocketAdapter createJobSubmitAndConnectWebSocket(SessionContext webSocketSessionCtx, Map<String, List<String>> qryParams) {
        return new JobSubmitAndConnectWebSocket(
                mantisClient, masterClientWrapper, webSocketSessionCtx.getStats(),
                qryParams, webSocketSessionCtx, remoteSinkConnector, propertyRepository, registry, workerThreadPool);
    }

    @Override
    public void doOptions(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "POST", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        EventSource eventSource = newEventSource(request, response);
        respond(request, response);
        AsyncContext async = request.startAsync();
        // Infinite timeout because the continuation is never resumed,
        // but only completed on close
        async.setTimeout(0);
        SSEWebSocketServletBase.EventSourceEmitter emitter = new SSEWebSocketServletBase.EventSourceEmitter(eventSource, async);
        emitter.scheduleHeartBeat();
        open(eventSource, emitter);

        setupStream(request, emitter);
    }

    public void setupStream(HttpServletRequest request, EventSource.Emitter emitter) {
        final Map<String, List<String>> queryParameters = HttpUtils.getQryParams(request.getQueryString());
        final SinkParameters sinkParameters;

        final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
        final SessionContext httpSessionCtx = contextBuilder.createHttpSessionCtx(request.getRemoteAddr(),
                request.getRequestURI() + "?" + request.getQueryString(), request.getMethod());

        try {
            Property<Boolean> isSourceSamplingEnabled = propertyRepository.get(PropertyNames.sourceSamplingEnabled, Boolean.class).orElse(false);
            sinkParameters = JobSinkConnector.getSinkParameters(queryParameters, false, isSourceSamplingEnabled);
        } catch (UnsupportedEncodingException e) {
            final String error = String.format("Error in sink connect request's queryParams [%s], error: %s",
                    queryParameters, e.getMessage());
            logger.error(error, e);
            try {
                emitter.data(String.format("Error in sink connect request's queryParams [%s], error: %s",
                        queryParameters, e.getMessage()));
            } catch (IOException e1) {
                logger.warn("Couldn't send error message to client (session id: " + httpSessionCtx.getId() +
                        "): error in sink connect request's queryParams " + queryParameters + ", error: " + e.getMessage());
            }
            return;
        }

        boolean sendTunnelPings = queryParameters != null && queryParameters.get(QueryParams.TunnelPingParamName) != null &&
                Boolean.valueOf(queryParameters.get(QueryParams.TunnelPingParamName).get(0));
        final List<Tag> tags = QueryParams.getTaglist(queryParameters, httpSessionCtx.getId(), request.getRequestURI());
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = request.getReader()) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                content.append(line);
            }
        } catch (IOException ex) {
            logger.error(ex.getMessage());
        }

        final AtomicBoolean hasErrored = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);

        Func0<Observable<Observable<MantisServerSentEvent>>> localResultsGetter =
                () -> JobSubmitAndConnect.submit(content.toString(), masterClientWrapper).take(1)
                        .map(jobId -> {
                            return MantisClientUtil.isJobId(jobId) ?
                                    JobSinkConnector.getResults(true, mantisClient, jobId, sinkParameters)
                                            .flatMap(o -> o) :
                                    JobSubmitAndConnect.verifyJobIdAndGetObservable(jobId);
                        });
        Func1<String, Observable<MantisServerSentEvent>> remoteResultsGetter = region ->
                remoteSinkConnector.postAndGetResults(
                        region, JobSubmitAndConnect.handlerName, sinkParameters, content.toString()
                );

        final ServletConx conx = JobSinkServletConnector.getServletConx(emitter, hasErrored, latch, httpSessionCtx);

        JobConnectWebSocket.process(sendTunnelPings, conx, httpSessionCtx,
                httpSessionCtx.getStats(), tags, "submit",
                localResultsGetter, remoteResultsGetter,
                s -> {
                    hasErrored.set(true);
                    latch.countDown();
                },
                hasErrored, registry, propertyRepository, workerThreadPool);
    }
}
