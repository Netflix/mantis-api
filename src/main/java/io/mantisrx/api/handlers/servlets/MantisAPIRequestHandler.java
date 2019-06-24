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

import java.util.List;
import java.util.Map;

import com.netflix.spectator.api.Tag;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.handlers.utils.QueryParams;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerResponse;
import rx.Observable;


public interface MantisAPIRequestHandler {

    public static class Request {

        public final String path;
        public final String uri;
        public final String queryString;
        public final Map<String, List<String>> queryParameters;
        public final List<Tag> tags;
        public final String content;
        public final HttpMethod httpMethod;
        public final SessionContext context;

        public Request(String path, String uri, String queryString, Map<String, List<String>> queryParameters, String content,
                       HttpMethod httpMethod, SessionContext context) {
            this.path = path;
            this.uri = uri;
            this.queryString = queryString;
            this.content = content;
            this.httpMethod = httpMethod;
            this.context = context;
            this.queryParameters = queryParameters;
            tags = QueryParams.getTaglist(queryParameters, context.getId(), context.getUri());
        }
    }

    public String getName();

    public String getHelp();

    public Observable<Void> handleOptions(HttpServerResponse<ByteBuf> response, Request request);

    public Observable<Void> handleGet(HttpServerResponse<ByteBuf> response, Request request);

    public Observable<Void> handlePost(HttpServerResponse<ByteBuf> response, Request request);
}
