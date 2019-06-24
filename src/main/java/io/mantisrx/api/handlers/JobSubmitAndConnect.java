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

package io.mantisrx.api.handlers;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.handlers.connectors.JobSinkConnector;
import io.mantisrx.api.handlers.connectors.RemoteSinkConnector;
import io.mantisrx.api.handlers.servlets.MantisAPIRequestHandler;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.LogUtils;
import io.mantisrx.api.handlers.utils.MantisClientUtil;
import io.mantisrx.api.handlers.utils.RetryUtils;
import io.mantisrx.client.MantisClient;
import io.mantisrx.common.MantisServerSentEvent;
import io.mantisrx.runtime.parameter.SinkParameters;
import io.mantisrx.server.master.client.MasterClientWrapper;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;


public class JobSubmitAndConnect implements MantisAPIRequestHandler {

    private static Logger logger = LoggerFactory.getLogger(JobSubmitAndConnect.class);
    private final MantisClient mantisClient;
    private final MasterClientWrapper masterClientWrapper;
    private static final String API_JOB_SUBMIT_PATH = "/api/submit";
    public static final String handlerName = "jobsubmitandconnect";
    private final RemoteSinkConnector remoteSinkConnector;
    private final ScheduledThreadPoolExecutor threadpool;
    private final PropertyRepository propertyRepository;

    public JobSubmitAndConnect(MantisClient mantisClient, MasterClientWrapper masterClientWrapper, RemoteSinkConnector remoteSinkConnector, ScheduledThreadPoolExecutor threadpool, PropertyRepository propertyRepository) {
        this.mantisClient = mantisClient;
        this.masterClientWrapper = masterClientWrapper;
        this.remoteSinkConnector = remoteSinkConnector;
        this.threadpool = threadpool;
        this.propertyRepository = propertyRepository;
    }

    @Override
    public String getName() {
        return handlerName;
    }

    @Override
    public String getHelp() {
        return "(POST) " + JobSubmitAndConnect.handlerName + " (body: refer to docs)";
    }

    @Override
    public Observable<Void> handleOptions(HttpServerResponse<ByteBuf> response, Request request) {
        HttpUtils.setBaseHeaders(response.getHeaders(), HttpMethod.POST, HttpMethod.OPTIONS);
        response.setStatus(HttpResponseStatus.NO_CONTENT);
        return response.close();
    }

    @Override
    public Observable<Void> handleGet(HttpServerResponse<ByteBuf> response, Request request) {
        return Observable.error(new UnsupportedOperationException("GET not supported"));
    }

    @Override
    public Observable<Void> handlePost(HttpServerResponse<ByteBuf> response, Request request) {
        logger.info(LogUtils.getPostStartMsg(this, request.context));
        final SinkParameters sinkParameters;
        try {
            Property<Boolean> isSourceSamplingEnabled = propertyRepository.get(PropertyNames.sourceSamplingEnabled, Boolean.class).orElse(true);
            sinkParameters = JobSinkConnector.getSinkParameters(request.queryParameters, false, isSourceSamplingEnabled);
        } catch (UnsupportedEncodingException e) {
            final String error = String.format("Error in sink connect request's queryParams [%s], error: %s",
                    request.queryParameters, e.getMessage());
            logger.error(error, e);
            return sendError(response, error);
        }
        Func0<Observable<Observable<MantisServerSentEvent>>> localResultsGetter =
                () -> submit(request.content, masterClientWrapper).take(1)
                        .map(jobId -> {
                            return MantisClientUtil.isJobId(jobId) ?
                                    JobSinkConnector.getResults(true, mantisClient, jobId, sinkParameters)
                                            .flatMap(o -> o) :
                                    verifyJobIdAndGetObservable(jobId);
                        });
        Func1<String, Observable<MantisServerSentEvent>> remoteResultsGetter = region ->
                remoteSinkConnector.postAndGetResults(
                        region, JobSubmitAndConnect.handlerName, sinkParameters, request.content
                );
        return JobSinkConnector.process(response, request, localResultsGetter, remoteResultsGetter, threadpool);
    }

    public static Observable<MantisServerSentEvent> verifyJobIdAndGetObservable(String jobId) {
        return isInvalidNamedJob(jobId) ?
                Observable.just(new MantisServerSentEvent(JobSinkConnector.MetaErrorMsgPrefix + MasterClientWrapper.InvalidNamedJob))
                        .mergeWith(Observable.timer(1, TimeUnit.SECONDS).take(1)
                                .flatMap(aLong -> Observable.<MantisServerSentEvent>error(new Exception(jobId))))
                :
                Observable.<MantisServerSentEvent>error(new Exception(jobId));
    }

    private static boolean isInvalidNamedJob(String jobId) {
        return jobId != null && jobId.startsWith("Error with job") && jobId.contains("named job doesn't exist");
    }

    public static Observable<String> submit(String content, MasterClientWrapper masterClientWrapper) {
        return MantisClientUtil
                .callPostOnMaster(masterClientWrapper.getMasterMonitor().getMasterObservable(), API_JOB_SUBMIT_PATH, content)
                .retryWhen(RetryUtils.getRetryFunc(logger))
                .flatMap(masterResponse -> masterResponse.getByteBuf()
                        .take(1)
                        .map(byteBuf -> {
                            final String s = byteBuf.toString(Charset.forName("UTF-8"));
                            logger.info("response: " + s);
                            return s;
                        }));
    }

    private Observable<Void> sendError(HttpServerResponse<ByteBuf> response, String s) {
        HttpUtils.setBaseHeaders(response.getHeaders(), HttpMethod.POST, HttpMethod.OPTIONS);
        response.setStatus(HttpResponseStatus.BAD_REQUEST);
        response.writeStringAndFlush(s);
        return response.close();
    }
}
