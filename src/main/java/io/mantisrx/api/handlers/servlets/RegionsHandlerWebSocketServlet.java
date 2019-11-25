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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.connectors.JobSinkServletConnector;
import io.mantisrx.api.handlers.connectors.RemoteSinkConnector;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.PathUtils;
import io.mantisrx.api.handlers.utils.Regions;
import io.mantisrx.api.handlers.ws.ErrorMsgWebSocket;
import io.mantisrx.api.tunnel.StreamingClientFactory;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.master.client.MasterClientWrapper;
import org.eclipse.jetty.servlets.EventSource;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RegionsHandlerWebSocketServlet extends SSEWebSocketServletBase {

    /**
     * generated ID
     */
    private static final long serialVersionUID = 7022259799964101622L;
    private static Logger logger = LoggerFactory.getLogger(RegionsHandlerWebSocketServlet.class);
    public static final String endpointName = "region";
    public static final String helpMsg = endpointName + "/<region_regex | all | remote>/<AnyOfTheOtherEndpoints>";
    private final JobConnectByIdWebSocketServlet connectByIdServlet;
    private final JobConnectByNameWebSocketServlet connectByNameServlet;
    private final JobSubmitAndConnectServlet submitAndConnectServlet;
    private transient final MantisClient mantisClient;
    private transient final MasterClientWrapper masterClientWrapper;
    private transient final RemoteSinkConnector remoteSinkConnector;
    private transient final StreamingClientFactory streamingClientFactory;
    private transient final PropertyRepository propertyRepository;
    private transient final Registry registry;
    private transient final WorkerThreadPool workerThreadPool;

    private final List<String> regions;

    public RegionsHandlerWebSocketServlet(MantisClient mantisClient, MasterClientWrapper masterClientWrapper, StreamingClientFactory streamingClientFactory, PropertyRepository propertyRepository, Registry registry, WorkerThreadPool workerThreadPool) {
        super(propertyRepository);
        this.mantisClient = mantisClient;
        this.masterClientWrapper = masterClientWrapper;
        this.streamingClientFactory = streamingClientFactory;
        this.propertyRepository = propertyRepository;
        this.registry = registry;
        this.workerThreadPool = workerThreadPool;
        this.remoteSinkConnector = new RemoteSinkConnector(streamingClientFactory, registry);

        this.connectByIdServlet = new JobConnectByIdWebSocketServlet(mantisClient, remoteSinkConnector, propertyRepository, registry, workerThreadPool);
        this.connectByNameServlet = new JobConnectByNameWebSocketServlet(mantisClient, remoteSinkConnector, propertyRepository, registry, workerThreadPool);
        this.submitAndConnectServlet = new JobSubmitAndConnectServlet(mantisClient, masterClientWrapper, remoteSinkConnector, registry, propertyRepository, workerThreadPool);

        String REGIONS_LIST = propertyRepository.get(PropertyNames.mantisAPIRegions, String.class).orElse("us-east-1,us-west-2,eu-west-1").get();
        List<String> rlist = new ArrayList<>();
        StringTokenizer tokenizer = new StringTokenizer(REGIONS_LIST, ",");
        while (tokenizer.hasMoreTokens())
            rlist.add(tokenizer.nextToken());
        regions = Collections.unmodifiableList(rlist);
        logger.info("Setup REGIONS=" + regions);
    }

    /**
     * TODO: I really don't like that this function mutates context. This caused some trouble when migrating to jetty 9.
     *
     * @param context
     * @param path
     * @param pathToken
     *
     * @return
     *
     * @throws Exception
     */
    private String setRegionsAndGetCmd(SessionContext context, String path, String pathToken) throws Exception {
        final String region = PathUtils.getTokenAfter(path, pathToken);
        final List<String> matchingRegions = Regions.getMatchingRegions(propertyRepository, region);
        if (matchingRegions != null && !matchingRegions.isEmpty())
            context.setRegions(matchingRegions);
        final String cmd = PathUtils.getTokenAfter(path, region);
        if (cmd == null || cmd.isEmpty()) {
            throw new Exception(String.format("Session %d: did not provide command after region token",
                    context.getId()));
        }
        return cmd;
    }

    private String getUriAfterRegion(String path, String endpointName) {
        final String region = PathUtils.getTokenAfter(path, endpointName);
        return PathUtils.getPathAfter(path, region);
    }

    @Override
    public void configure(WebSocketServletFactory factory) {
        super.configure(factory);
        factory.setCreator((ServletUpgradeRequest req, ServletUpgradeResponse resp) -> {
            logger.info("Websocket handling request path=" + req.getRequestURI());
            final Map<String, List<String>> qryParams = HttpUtils.getQryParams(req.getQueryString());
            final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
            final SessionContext webSocketSessionCtx = contextBuilder.createWebSocketSessionCtx(req.getRemoteAddress(),
                    req.getRequestURI() + "?" + req.getQueryString());
            String cmd = "";
            try {
                cmd = setRegionsAndGetCmd(webSocketSessionCtx, req.getRequestURI().toString(), "/" + endpointName);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return new ErrorMsgWebSocket(e.getMessage(), webSocketSessionCtx);
            }
            String target = PathUtils.getTokenAfter(req.getRequestURI().toString(), cmd);

            switch (cmd) {
            case JobConnectByIdWebSocketServlet.handlerName:
                return connectByIdServlet.createJobConnectWebSocket(webSocketSessionCtx, qryParams, target);
            case JobConnectByNameWebSocketServlet.handlerName:
                return connectByNameServlet.createJobConnectWebSocket(webSocketSessionCtx, qryParams, target);
            case JobSubmitAndConnectServlet.endpointName:
                return submitAndConnectServlet.createJobSubmitAndConnectWebSocket(webSocketSessionCtx, qryParams);
            default:
                return new ErrorMsgWebSocket("Invalid request: " + req.getRequestURI(), webSocketSessionCtx);
            }
        });
    }

    @Override
    public void doOptions(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS", "POST");
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
        final SessionContext httpSessionCtx = contextBuilder.createHttpSessionCtx(request.getRemoteAddr(),
                request.getRequestURI() + "?" + request.getQueryString(), request.getMethod());

        String cmd = "";
        try {
            cmd = setRegionsAndGetCmd(httpSessionCtx, request.getRequestURI(), "/" + endpointName);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getOutputStream().print("error: " + e.getMessage());
            response.flushBuffer();
            return;
        }

        final Map<String, List<String>> queryParams = HttpUtils.getQryParams(request.getQueryString());
        final String target = PathUtils.getTokenAfter(request.getRequestURI(), cmd);

        switch (cmd) {
        case JobConnectByIdWebSocketServlet.handlerName:
            JobSinkServletConnector sinkServletConnector = new JobSinkServletConnector(mantisClient, true,
                    httpSessionCtx.getStats(), queryParams, httpSessionCtx, remoteSinkConnector, propertyRepository, registry, workerThreadPool);
            EventSource.Emitter emitter = getEmitter(request, response);
            sinkServletConnector.connect(target, emitter);
            break;
        case JobConnectByNameWebSocketServlet.handlerName:
            sinkServletConnector = new JobSinkServletConnector(mantisClient, false, httpSessionCtx.getStats(),
                    queryParams, httpSessionCtx, remoteSinkConnector, propertyRepository, registry, workerThreadPool);
            emitter = getEmitter(request, response);
            sinkServletConnector.connect(target, emitter);
            break;
        default:
            // relay endpoint
            MantisMasterEndpointRelayServlet mantisMasterEndpointRelayServlet = new MantisMasterEndpointRelayServlet(masterClientWrapper, streamingClientFactory, propertyRepository, registry, workerThreadPool);
            final String path = getUriAfterRegion(request.getRequestURI(), endpointName);
            mantisMasterEndpointRelayServlet.handleGetWithRegions(request, response, path, httpSessionCtx);
        }
    }

    private EventSource.Emitter getEmitter(HttpServletRequest request, HttpServletResponse response) throws IOException {
        EventSource eventSource = newEventSource(request, response);
        respond(request, response);
        AsyncContext async = request.startAsync();
        // Infinite timeout because the continuation is never resumed,
        // but only completed on close
        async.setTimeout(0);
        SSEWebSocketServletBase.EventSourceEmitter emitter = new SSEWebSocketServletBase.EventSourceEmitter(eventSource, async);
        emitter.scheduleHeartBeat();
        open(eventSource, emitter);
        return emitter;
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
        final SessionContext httpSessionCtx = contextBuilder.createHttpSessionCtx(request.getRemoteAddr(),
                request.getRequestURI() + "?" + request.getQueryString(), request.getMethod());
        String cmd = "";
        try {
            cmd = setRegionsAndGetCmd(httpSessionCtx, request.getRequestURI(), "/" + endpointName);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getOutputStream().print("error: " + e.getMessage());
            response.flushBuffer();
            return;
        }

        switch (cmd) {
        case JobSubmitAndConnectServlet.endpointName:
            JobSubmitAndConnectServlet jscs = new JobSubmitAndConnectServlet(mantisClient, masterClientWrapper, remoteSinkConnector, registry, propertyRepository, workerThreadPool);
            EventSource.Emitter emitter = getEmitter(request, response);
            jscs.setupStream(request, emitter);
            break;
        default:
            // relay endpoint
            MantisMasterEndpointRelayServlet mantisMasterEndpointRelayServlet = new MantisMasterEndpointRelayServlet(masterClientWrapper, streamingClientFactory, propertyRepository, registry, workerThreadPool);
            final String path = getUriAfterRegion(request.getRequestURI(), endpointName);
            StringBuilder content = new StringBuilder();
            try (BufferedReader reader = request.getReader()) {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    content.append(line);
                }
            }
            mantisMasterEndpointRelayServlet.handlePostWithRegions(request, response, path, content.toString(), httpSessionCtx);
        }
    }
}
