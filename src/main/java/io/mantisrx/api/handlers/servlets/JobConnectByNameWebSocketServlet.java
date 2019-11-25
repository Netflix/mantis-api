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
import java.util.List;
import java.util.Map;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.connectors.JobSinkServletConnector;
import io.mantisrx.api.handlers.connectors.RemoteSinkConnector;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.PathUtils;
import io.mantisrx.api.handlers.ws.JobConnectWebSocket;
import io.mantisrx.client.MantisClient;
import org.eclipse.jetty.servlets.EventSource;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobConnectByNameWebSocketServlet extends SSEWebSocketServletBase {

    @SuppressWarnings("unused")

    private static Logger logger = LoggerFactory.getLogger(JobConnectByNameWebSocketServlet.class);
    private transient final MantisClient mantisClient;
    private transient final RemoteSinkConnector remoteSinkConnector;
    public static final String handlerName = "jobconnectbyname";
    public static final String helpMsg = handlerName + "/<JobName>";

    private static final long serialVersionUID = JobConnectByNameWebSocketServlet.class.hashCode();
    private transient final Registry registry;
    private transient final PropertyRepository propertyRepository;
    private transient final WorkerThreadPool workerThreadPool;

    public JobConnectByNameWebSocketServlet(MantisClient mantisClient, RemoteSinkConnector remoteSinkConnector, PropertyRepository propertyRepository, Registry registry, WorkerThreadPool workerThreadPool) {
        super(propertyRepository);
        this.mantisClient = mantisClient;
        this.remoteSinkConnector = remoteSinkConnector;
        this.propertyRepository = propertyRepository;
        this.registry = registry;
        this.workerThreadPool = workerThreadPool;
    }

    @Override
    public void configure(WebSocketServletFactory factory) {
        super.configure(factory);
        factory.setCreator((ServletUpgradeRequest req, ServletUpgradeResponse resp) -> {
            final Map<String, List<String>> qryParams = HttpUtils.getQryParams(req.getQueryString());
            final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
            final SessionContext webSocketSessionCtx = contextBuilder.createWebSocketSessionCtx(req.getRemoteAddress(),
                    req.getRequestURI() + "?" + req.getQueryString());
            return createJobConnectWebSocket(webSocketSessionCtx, qryParams, PathUtils.getTokenAfter(req.getRequestPath(), "/" + handlerName));
        });
    }

    public WebSocketAdapter createJobConnectWebSocket(SessionContext webSocketSessionCtx, Map<String, List<String>> qryParams, String target) {
        final JobConnectWebSocket jobConnectWebSocket = new JobConnectWebSocket(
                false,
                mantisClient,
                webSocketSessionCtx.getStats(),
                qryParams,
                webSocketSessionCtx,
                remoteSinkConnector,
                registry,
                propertyRepository,
                workerThreadPool);
        jobConnectWebSocket.setTarget(target);
        return jobConnectWebSocket;
    }

    @Override
    public void doOptions(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        EventSource eventSource = newEventSource(request, response);
        super.respond(request, response);
        AsyncContext async = request.startAsync();
        // Infinite timeout because the continuation is never resumed,
        // but only completed on close
        async.setTimeout(0);
        SSEWebSocketServletBase.EventSourceEmitter emitter = new SSEWebSocketServletBase.EventSourceEmitter(eventSource, async);
        emitter.scheduleHeartBeat();
        open(eventSource, emitter);

        final Map<String, List<String>> qryParams = HttpUtils.getQryParams(request.getQueryString());
        final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
        final SessionContext httpSessionCtx = contextBuilder.createHttpSessionCtx(request.getRemoteAddr(),
                request.getRequestURI() + "?" + request.getQueryString(), request.getMethod());
        final JobSinkServletConnector connector =
                new JobSinkServletConnector(mantisClient, false, httpSessionCtx.getStats(), qryParams, httpSessionCtx, remoteSinkConnector, propertyRepository, registry, workerThreadPool);
        connector.connect(PathUtils.getTokenAfter(request.getPathInfo(), ""), emitter);
    }
}
