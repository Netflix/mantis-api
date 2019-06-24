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
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.JobSubmitAndConnect;
import io.mantisrx.api.handlers.connectors.JobSinkConnector;
import io.mantisrx.api.handlers.connectors.RemoteSinkConnector;
import io.mantisrx.api.handlers.utils.MantisClientUtil;
import io.mantisrx.api.handlers.utils.QueryParams;
import io.mantisrx.api.handlers.utils.RetryUtils;
import io.mantisrx.api.metrics.Stats;
import io.mantisrx.client.MantisClient;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.mantisrx.server.master.client.MasterClientWrapper;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func0;
import rx.functions.Func1;


public class JobSubmitAndConnectWebSocket extends WebSocketAdapter {

    private static Logger logger = LoggerFactory.getLogger(JobSubmitAndConnectWebSocket.class);
    private final MantisClient mantisClient;
    private final MasterClientWrapper masterClientWrapper;
    private final Stats stats;
    private final List<Tag> tags;
    private final Map<String, List<String>> qryParams;
    private final SessionContext sessionContext;
    private static final String API_JOB_SUBMIT_PATH = "/api/submit";
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private volatile Subscription subscription = null;
    private volatile AtomicBoolean closed = new AtomicBoolean(false);
    private SinkParameters sinkParameters = null;
    private RemoteSinkConnector remoteSinkConnector;
    private final boolean sendTunnelPings;
    private final PropertyRepository propertyRepository;
    public final Registry registry;
    public final WorkerThreadPool workerThreadPool;

    public JobSubmitAndConnectWebSocket(MantisClient mantisClient, MasterClientWrapper masterClientWrapper,
                                        Stats stats, Map<String, List<String>> qryParams, SessionContext sessionContext, RemoteSinkConnector remoteSinkConnector, PropertyRepository propertyRepository,
                                        Registry registry, WorkerThreadPool workerThreadPool) {
        this.mantisClient = mantisClient;
        this.masterClientWrapper = masterClientWrapper;
        this.stats = stats;
        this.qryParams = qryParams;
        this.sessionContext = sessionContext;
        this.remoteSinkConnector = remoteSinkConnector;
        this.propertyRepository = propertyRepository;
        this.registry = registry;
        this.workerThreadPool = workerThreadPool;

        sendTunnelPings = qryParams != null && qryParams.get(QueryParams.TunnelPingParamName) != null &&
                Boolean.valueOf(qryParams.get(QueryParams.TunnelPingParamName).get(0));
        tags = QueryParams.getTaglist(qryParams, sessionContext.getId(), sessionContext.getUri());
    }

    @Override
    public void onWebSocketText(String message) {
        if (!connected.compareAndSet(false, true)) {
            final String errorMsg = String.format("Already received 1 of 1 expected message, unexpected to receive new message: %s", message);
            try {
                this.getSession().getRemote().sendString(errorMsg);
            } catch (IOException e) {
                logger.warn(String.format("Couldn't send error (%s) to client session id %d: %s", errorMsg, sessionContext.getId(), e.getMessage()));
            }
            return;
        }
        Func0<Observable<Observable<MantisServerSentEvent>>> localResultsGetter =
                () -> getLocalResults(message, masterClientWrapper, mantisClient, sinkParameters);
        Func1<String, Observable<MantisServerSentEvent>> remoteResultsGetter = region ->
                remoteSinkConnector.postAndGetResults(region, JobSubmitAndConnect.handlerName, sinkParameters, message);
        subscription = JobConnectWebSocket.process(sendTunnelPings, WebsocketUtils.getWSConx(this.getSession()),
                sessionContext, stats, tags,
                "submit", localResultsGetter, remoteResultsGetter, s -> this.onWebSocketClose(-1, s), closed,
                registry, propertyRepository, workerThreadPool);
    }

    static Observable<Observable<MantisServerSentEvent>> getLocalResults(String data, MasterClientWrapper masterClientWrapper, MantisClient mantisClient, SinkParameters sinkParameters) {
        return MantisClientUtil
                .callPostOnMaster(masterClientWrapper.getMasterMonitor().getMasterObservable(), API_JOB_SUBMIT_PATH, data)
                .retryWhen(RetryUtils.getRetryFunc(logger, Integer.MAX_VALUE))
                .map(masterResponse -> masterResponse.getByteBuf()
                        .take(1)
                        .flatMap(byteBuf -> {
                            final String jobId = byteBuf.toString(Charset.forName("UTF-8"));
                            logger.info("response: " + jobId);
                            if (MantisClientUtil.isJobId(jobId))
                                return JobSinkConnector.getResults(true, mantisClient, jobId, sinkParameters).flatMap(o -> o);
                            return JobSubmitAndConnect.verifyJobIdAndGetObservable(jobId);
                        }));
    }

    @Override
    public void onWebSocketConnect(Session sess) {
        try {
            Property<Boolean> isSourceSamplingEnabled = propertyRepository.get(PropertyNames.sourceSamplingEnabled, Boolean.class).orElse(false);
            sinkParameters = JobSinkConnector.getSinkParameters(qryParams, false, isSourceSamplingEnabled);
        } catch (UnsupportedEncodingException e) {
            try {
                logger.error("Can't get sink parameters: " + e.getMessage(), e);
                sess.getRemote().sendString("error: can't create sink parameters: " + e.getMessage());
            } catch (IOException e1) {
                logger.error(String.format("Error sending error message (%s) to client: %s", e.getMessage(), e1.getMessage()), e1);
            }
            sess.close();
        }
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        closed.set(true);
        if (subscription != null)
            subscription.unsubscribe();
        if (sessionContext != null)
            sessionContext.endSession();
    }
}
