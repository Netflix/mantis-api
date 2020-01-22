/*
 * Copyright 2018 Netflix, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */
package io.mantisrx.api.initializers;

import com.netflix.netty.common.HttpLifecycleChannelHandler;
import com.netflix.zuul.netty.server.BaseZuulChannelInitializer;

import io.mantisrx.api.push.ConnectionBroker;
import io.mantisrx.api.push.MantisSSEHandler;
import io.mantisrx.api.push.MantisWebSocketFrameHandler;
import io.mantisrx.api.util.Util;
import io.mantisrx.client.MantisClient;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import com.netflix.netty.common.channel.config.ChannelConfig;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class MantisApiServerChannelInitializer extends BaseZuulChannelInitializer {

    private final ConnectionBroker connectionBroker;
    private final MasterClientWrapper masterClientWrapper;
    private final List<String> pushPrefixes;

    public MantisApiServerChannelInitializer(
            String metricId,
            ChannelConfig channelConfig,
            ChannelConfig channelDependencies,
            ChannelGroup channels,
            List<String> pushPrefixes,
            MantisClient mantisClient,
            MasterClientWrapper masterClientWrapper) {

        super(metricId, channelConfig, channelDependencies, channels);
        this.pushPrefixes = pushPrefixes;
        this.connectionBroker = new ConnectionBroker(mantisClient);
        this.masterClientWrapper = masterClientWrapper;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception
    {
        ChannelPipeline pipeline = ch.pipeline();

        storeChannel(ch);
        addTimeoutHandlers(pipeline);
        addPassportHandler(pipeline);
        addTcpRelatedHandlers(pipeline);
        addHttp1Handlers(pipeline);
        addHttpRelatedHandlers(pipeline);

        pipeline.addLast("mantishandler", new MantisChannelHandler(pushPrefixes));
    }

    /**
     * Adds a series of handlers for providing SSE/Websocket connections
     * to Mantis Jobs.
     *
     * TODO: Most of these can be created once and reused except WebSocketServerProtocolHandler.
     *
     * @param pipeline The netty pipeline to which push handlers should be added.
     * @param url The url with which to initiate the websocket handler.
     */
    protected void addPushHandlers(final ChannelPipeline pipeline, String url) {
        pipeline.addLast(new ChunkedWriteHandler());
        pipeline.addLast(new HttpObjectAggregator(64 * 1024));
        pipeline.addLast(new MantisSSEHandler(connectionBroker, masterClientWrapper, pushPrefixes));
        pipeline.addLast(new WebSocketServerProtocolHandler(url, true));
        pipeline.addLast(new MantisWebSocketFrameHandler(connectionBroker));
    }

    /**
     * The MantisChannelHandler's job is to initialize the tail end of the pipeline differently
     * depending on the URI of the request. This is largely to circumvent issues with endpoint responses
     * when the push handlers preceed the Zuul handlers.
     */
    @Sharable
    public class MantisChannelHandler extends ChannelInboundHandlerAdapter {

        private final List<String> pushPrefixes;

        public MantisChannelHandler(List<String> pushPrefixes) {
            this.pushPrefixes = pushPrefixes;
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof HttpLifecycleChannelHandler.StartEvent) {
                HttpLifecycleChannelHandler.StartEvent startEvent = (HttpLifecycleChannelHandler.StartEvent) evt;
                String uri = startEvent.getRequest().uri();
                ChannelPipeline pipeline = ctx.pipeline();

                removeEverythingAfterThis(pipeline);

                if (Util.startsWithAnyOf(uri, this.pushPrefixes)) {
                    addPushHandlers(pipeline, uri);
                } else {
                    addZuulHandlers(pipeline);
                }
            }
            ctx.fireUserEventTriggered(evt);
        }
    }

    private void removeEverythingAfterThis(ChannelPipeline pipeline) {
        while (pipeline.last().getClass() != MantisChannelHandler.class) {
            pipeline.removeLast();
        }
    }
}
