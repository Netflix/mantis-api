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

import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.SpectatorUtils;
import io.mantisrx.api.handlers.utils.QueryParams;
import io.mantisrx.api.handlers.utils.RetryUtils;
import io.mantisrx.api.handlers.utils.TunnelUtils;
import io.mantisrx.api.tunnel.StreamingClientFactory;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SinkParameter;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import mantis.io.reactivex.netty.channel.StringTransformer;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.sse.ServerSentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


public class RemoteSinkConnector {

    private static final Logger logger = LoggerFactory.getLogger(RemoteSinkConnector.class);
    private static final String numRemoteBytesCounterName = "numRemoteSinkBytes";
    private static final String numRemoteMessagesCounterName = "numRemoteMessages";
    static final String numSseErrors = "numSseErrors";

    private final StreamingClientFactory streamingClientFactory;
    private final Registry registry;

    public RemoteSinkConnector(StreamingClientFactory streamingClientFactory, Registry registry) {
        this.streamingClientFactory = streamingClientFactory;
        this.registry = registry;
    }

    public Observable<MantisServerSentEvent> postAndGetResults(String region, String endpoint, SinkParameters sinkParameters, String content) {
        final HttpClient<ByteBuf, ServerSentEvent> client = streamingClientFactory.getSecureSseClient(region);

        final String uri = createUrl(endpoint, sinkParameters);
        HttpClientRequest<ByteBuf> request = HttpClientRequest.create(HttpMethod.POST, uri);
        request.withRawContent(content, StringTransformer.DEFAULT_INSTANCE);
        logger.info("Remote POST with uri=" + uri);
        return client.submit(request)
                .retryWhen(RetryUtils.getRetryFunc(logger, Integer.MAX_VALUE))
                .flatMap(response -> {
                    if (response.getStatus().reasonPhrase().equals("OK"))
                        return response.getContent();
                    return Observable.<ServerSentEvent>error(
                            new Exception(response.getStatus().reasonPhrase())
                    );
                })
                .map(sse -> new MantisServerSentEvent(sse.contentAsString()))
                ;
    }

    public Observable<MantisServerSentEvent> getResults(String region, String endpoint, String target, SinkParameters
            sinkParameters) {
        final HttpClient<ByteBuf, ServerSentEvent> clientWithSsl = streamingClientFactory.getSecureSseClient(region);
        final String uri = createUrl(endpoint + "/" + target, sinkParameters);
        logger.info("Connecting with ssl URI=" + uri);
        return clientWithSsl
                .submit(HttpClientRequest.createGet(uri))
                .retryWhen(RetryUtils.getRetryFunc(logger, Integer.MAX_VALUE))
                .doOnError(throwable -> logger.warn(
                        String.format("Error getting response from remote SSE server for job %s in region %s: %s",
                                target, region, throwable.getMessage()), throwable
                )).flatMap(response -> {
                    if (!response.getStatus().reasonPhrase().equals("OK")) {
                        logger.warn(
                                String.format("Unexpected response from remote sink for job %s region %s: %s",
                                        target, region, response.getStatus().reasonPhrase()));
                        String err = response.getHeaders().get(JobSinkConnector.MetaErrorMsgHeader);
                        if (err == null || err.isEmpty())
                            err = response.getStatus().reasonPhrase();
                        return Observable.<MantisServerSentEvent>error(new Exception(err));
                    }
                    return streamContent(response, region, hasTunnelPingParam(sinkParameters))
                            .doOnError(t -> logger.error(t.getMessage()));
                })
                .doOnError(t -> logger.warn("Error streaming in remote data, will retry: " + t.getMessage(), t))
                .doOnCompleted(() -> logger.info(String.format("remote sink connection complete for job %s, region=%s", target, region)))
                ;
    }

    private boolean hasTunnelPingParam(SinkParameters sinkParameters) {
        if (sinkParameters != null) {
            for (SinkParameter p : sinkParameters.getSinkParams())
                if (QueryParams.TunnelPingParamName.equals(p.getName()) &&
                        Boolean.valueOf(p.getValue()))
                    return true;
        }
        return false;
    }

    private String createUrl(String endpoint, SinkParameters sinkParameters) {
        StringBuilder b = new StringBuilder("/").append(endpoint);
        final String paramStr = (sinkParameters == null) ? "" : sinkParameters.toString();
        if (paramStr.isEmpty())
            b.append("?").append(QueryParams.getTunnelConnectParams());
        else
            b.append(paramStr).append("&").append(QueryParams.getTunnelConnectParams());
        return b.toString();
    }


    private Observable<MantisServerSentEvent> streamContent(HttpClientResponse<ServerSentEvent> response, String
            region, boolean passThruTunnelPings) {
        Counter numRemoteBytes = SpectatorUtils.buildAndRegisterCounter(registry, numRemoteBytesCounterName, "region", region);
        Counter numRemoteMessages = SpectatorUtils.buildAndRegisterCounter(registry, numRemoteMessagesCounterName, "region", region);
        return response.getContent()
                .doOnError(t -> logger.warn(t.getMessage()))
                .timeout(3 * TunnelUtils.TunnelPingIntervalSecs, TimeUnit.SECONDS)
                .doOnError(t -> logger.warn("Timeout getting data from remote conx"))
                .filter(sse -> !(!sse.hasEventType() || !sse.getEventTypeAsString().startsWith("error:")) ||
                        passThruTunnelPings || !TunnelUtils.TunnelPingMessage.equals(sse.contentAsString()))
                .map(t1 -> {
                    String data = "";
                    if (t1.hasEventType() && t1.getEventTypeAsString().startsWith("error:")) {
                        logger.error("SSE has error, type=" + t1.getEventTypeAsString() + ", content=" + t1.contentAsString());
                        SpectatorUtils.buildAndRegisterCounter(registry, numSseErrors, "region", region).increment();
                        throw new RuntimeException("Got error SSE event: " + t1.contentAsString());
                    }
                    try {
                        data = t1.contentAsString();
                        if (data != null) {
                            numRemoteBytes.increment(data.length());
                            numRemoteMessages.increment();
                        }
                    } catch (Exception e) {
                        logger.error("Could not extract data from SSE " + e.getMessage(), e);
                    }
                    return new MantisServerSentEvent(data);
                });
    }
}
