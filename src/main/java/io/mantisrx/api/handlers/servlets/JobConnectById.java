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

import com.netflix.archaius.api.PropertyRepository;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.connectors.JobSinkConnector;
import io.mantisrx.api.handlers.connectors.RemoteSinkConnector;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.LogUtils;
import io.mantisrx.api.handlers.utils.PathUtils;
import io.mantisrx.client.MantisClient;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


public class JobConnectById implements MantisAPIRequestHandler {

    private static Logger logger = LoggerFactory.getLogger(JobConnectById.class);
    public final static String handlerName = "jobconnectbyid";
    private final JobSinkConnector sinkConnector;

    public JobConnectById(MantisClient mantisClient, RemoteSinkConnector remoteSinkConnector, PropertyRepository propertyRepository, WorkerThreadPool workerThreadPool) {
        sinkConnector = new JobSinkConnector(mantisClient, true, remoteSinkConnector, propertyRepository, workerThreadPool);
    }

    @Override
    public String getHelp() {
        return JobConnectById.handlerName + "/<JobID>";
    }

    @Override
    public Observable<Void> handleOptions(HttpServerResponse<ByteBuf> response, Request request) {
        HttpUtils.setBaseHeaders(response.getHeaders(), HttpMethod.GET, HttpMethod.OPTIONS);
        response.setStatus(HttpResponseStatus.NO_CONTENT);
        return response.close();
    }

    @Override
    public String getName() {
        return handlerName;
    }

    @Override
    public Observable<Void> handleGet(HttpServerResponse<ByteBuf> response, Request request) {
        final String jobId = PathUtils.getTokenAfter(request.path, "/" + getName());
        logger.info(LogUtils.getGetStartMsg(this, request.context));
        return sinkConnector.connect(jobId, response, request);
    }

    @Override
    public Observable<Void> handlePost(HttpServerResponse<ByteBuf> response, Request request) {
        return Observable.error(new UnsupportedOperationException("POST not supported"));
    }
}
