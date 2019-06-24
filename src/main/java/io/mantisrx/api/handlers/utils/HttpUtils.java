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

package io.mantisrx.api.handlers.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.logging.LogLevel;
import mantis.io.reactivex.netty.RxNetty;
import mantis.io.reactivex.netty.channel.StringTransformer;
import mantis.io.reactivex.netty.pipeline.PipelineConfigurator;
import mantis.io.reactivex.netty.protocol.http.client.HttpClient;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientPipelineConfigurator;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientRequest;
import mantis.io.reactivex.netty.protocol.http.client.HttpClientResponse;
import mantis.io.reactivex.netty.protocol.http.server.HttpResponseHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;


public class HttpUtils {

    private static Logger logger = LoggerFactory.getLogger(HttpUtils.class);

    public static void addBaseHeaders(HttpServletResponse resp, String... methods) {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Accept, Content-Type, Cache-Control");
        StringBuilder builder = new StringBuilder();
        if (methods != null && methods.length > 0) {
            for (String method : methods)
                builder.append(method).append(", ");
            String methodsVal = builder.toString();
            resp.setHeader("Access-Control-Allow-Methods", methodsVal.substring(0, methodsVal.lastIndexOf(',')));
        }
    }

    public static void addSseHeaders(HttpServletResponse resp) {
        resp.setHeader("content-type", "text/event-stream");
        resp.setHeader("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
        resp.setHeader("Pragma", "no-cache");
    }

    public static void setBaseHeaders(HttpResponseHeaders headers, HttpMethod... methods) {
        headers.set("Access-Control-Allow-Origin", "*");
        headers.set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Accept, Content-Type, Cache-Control");
        StringBuilder builder = new StringBuilder();
        if (methods != null && methods.length > 0) {
            for (HttpMethod method : methods)
                builder.append(method).append(", ");
            String methodsVal = builder.toString();
            headers.set("Access-Control-Allow-Methods", methodsVal.substring(0, methodsVal.lastIndexOf(',')));
        }
    }

    public static void setSseHeaders(HttpResponseHeaders headers) {
        headers.set("content-type", "text/event-stream");
        headers.set("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate");
        headers.set("Pragma", "no-cache");
    }

    public static Map<String, List<String>> getQryParams(String s) {
        Map<String, List<String>> result = new HashMap<>();
        if (s != null) {
            StringTokenizer tokenizer = new StringTokenizer(s, "&");
            while (tokenizer.hasMoreTokens()) {
                String p = tokenizer.nextToken();
                if (p != null) {
                    String[] pair = getStringPair(p, '=');
                    result.computeIfAbsent(pair[0], k -> new ArrayList<>());
                    try {
                        result.get(pair[0]).add(URLDecoder.decode(pair[1], "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        logger.warn("can't URL decode " + pair[1] + ": " + e.getMessage());
                        result.get(pair[0]).add(""); // unexpected
                    }
                }
            }
        }
        return result;
    }

    public static String[] getStringPair(String text, char delimiter) {
        int i = text.indexOf(delimiter);
        if (i < 0)
            return new String[] {text, ""};
        return new String[] {text.substring(0, i), text.substring(i + 1)};
    }

    public static String getGetResponseFromMantisAPI(String hostname, int port, String endpoint, long timeoutSecs) {
        return getResponseFromMantisAPI(false, "", hostname, port, endpoint, timeoutSecs);
    }

    public static String getPostReponseFromMantisAPI(String content, String hostname, int port, String endpoint,
                                                     long timeoutSecs) {
        return getResponseFromMantisAPI(true, content, hostname, port, endpoint, timeoutSecs);
    }

    private static String getResponseFromMantisAPI(boolean isPost, String content,
                                                   String hostname, int port, String endpoint, long timeoutSecs) {
        PipelineConfigurator<HttpClientResponse<ByteBuf>, HttpClientRequest<String>> pipelineConfigurator
                = new HttpClientPipelineConfigurator<>();
        HttpClient<String, ByteBuf> client =
                RxNetty.<String, ByteBuf>newHttpClientBuilder(hostname, port)
                        .pipelineConfigurator(pipelineConfigurator)
                        .enableWireLogging(LogLevel.TRACE)
                        .build();
        HttpClientRequest<String> request = isPost ?
                HttpClientRequest.create(HttpMethod.POST, endpoint) :
                HttpClientRequest.create(HttpMethod.GET, endpoint);
        if (isPost)
            request.withRawContent(content, StringTransformer.DEFAULT_INSTANCE);
        return client.submit(request)
                .flatMap(response -> response.getContent())
                .map(byteBuf -> byteBuf.toString(Charset.forName("UTF-8")))
                .onErrorResumeNext(throwable -> {
                    logger.warn("Can't get response from server " + hostname + ":" + port +
                            endpoint + ": " + throwable.getMessage());
                    return Observable.<String>empty();
                })
                .timeout(timeoutSecs, TimeUnit.SECONDS)
                .onErrorResumeNext(throwable -> {
                    logger.warn("Timed out getting response from server " + hostname + ":" + port +
                            endpoint + ": " + throwable.getMessage());
                    return Observable.<String>empty();
                })
                .collect(StringBuilder::new, StringBuilder::append)
                .toBlocking()
                .first().toString();
    }

    public static String getUrlMinusQueryStr(String uri) {
        if (uri == null || uri.isEmpty())
            return "NullTag";
        final int i = uri.indexOf('?');
        return i < 0 ? uri : uri.substring(0, i);
    }
}
