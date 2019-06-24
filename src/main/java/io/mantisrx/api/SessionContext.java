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

package io.mantisrx.api;

import java.util.List;

import io.mantisrx.api.metrics.Stats;
import io.netty.buffer.ByteBuf;
import mantis.io.reactivex.netty.protocol.http.server.HttpServerResponse;
import rx.functions.Action0;


public interface SessionContext {

    public enum ConxType {
        Http,
        WebSocket
    }

    public class ConxRefs {

        private final ConxType type;
        private HttpServerResponse<ByteBuf> httpResponse;

        public ConxRefs(ConxType type) {
            this.type = type;
        }

        public HttpServerResponse<ByteBuf> getHttpResponse() {
            return httpResponse;
        }

        public void setHttpResponse(HttpServerResponse<ByteBuf> httpResponse) {
            this.httpResponse = httpResponse;
        }

        public ConxType getType() {
            return type;
        }
    }

    long getId();

    long createTime();

    String getRemoteAddress();

    String getUri();

    String getMethod();

    Stats getStats();

    void endSession();

    ConxRefs getConxRefs();

    List<String> getRegions();

    void setRegions(List<String> regions);

    void setEndSessionListener(Action0 action);

    Action0 getEndSessionListener();
}
