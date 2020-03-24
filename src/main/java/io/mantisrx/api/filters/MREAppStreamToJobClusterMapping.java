/**
 * Copyright 2018 Netflix, Inc.
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
package io.mantisrx.api.filters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.mantis.discovery.proto.AppJobClustersMap;
import com.netflix.zuul.filters.http.HttpSyncEndpoint;
import com.netflix.zuul.message.http.HttpHeaderNames;
import com.netflix.zuul.message.http.HttpRequestMessage;
import com.netflix.zuul.message.http.HttpResponseMessage;
import com.netflix.zuul.message.http.HttpResponseMessageImpl;
import io.mantisrx.api.services.AppStreamDiscoveryService;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vavr.control.Try;

import java.util.List;

public class MREAppStreamToJobClusterMapping extends HttpSyncEndpoint {

    private final AppStreamDiscoveryService appStreamDiscoveryService;
    private final ObjectMapper objectMapper;

    private static final String APPNAME_QUERY_PARAM = "app";
    public static final String PATH_SPEC = "/api/v1/mantis/publish/streamJobClusterMap";

    @Inject
    public MREAppStreamToJobClusterMapping(AppStreamDiscoveryService appStreamDiscoveryService,
                                           ObjectMapper objectMapper) {
        this.appStreamDiscoveryService = appStreamDiscoveryService;
        this.objectMapper = objectMapper;
    }

    @Override
    public HttpResponseMessage apply(HttpRequestMessage request) {
        List<String> apps = request.getQueryParams().get(APPNAME_QUERY_PARAM);
        AppJobClustersMap payload = appStreamDiscoveryService.getAppJobClustersMap(apps);

        Try<String> serialized = Try.of(() -> objectMapper.writeValueAsString(payload));

        return serialized.map(body -> {
            HttpResponseMessage resp = new HttpResponseMessageImpl(request.getContext(), request, 200);
            resp.setBodyAsText(body);
            resp.getHeaders().set(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.APPLICATION_JSON.toString());
            return resp;
        }).getOrElseGet(t -> {
            HttpResponseMessage resp = new HttpResponseMessageImpl(request.getContext(), request, 200);
            resp.setBodyAsText(t.getMessage());
            resp.getHeaders().set(HttpHeaderNames.CONTENT_TYPE.toString(), HttpHeaderValues.TEXT_PLAIN.toString());
            return resp;
        });
    }
}
