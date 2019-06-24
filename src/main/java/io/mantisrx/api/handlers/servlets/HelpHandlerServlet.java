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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.spectator.api.Registry;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.handlers.utils.HttpUtils;
import io.mantisrx.api.handlers.utils.MantisClientUtil;
import io.mantisrx.api.handlers.utils.RetryUtils;
import io.mantisrx.server.core.master.MasterMonitor;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func1;


public class HelpHandlerServlet extends HttpServlet {

    private static Logger logger = LoggerFactory.getLogger(HelpHandlerServlet.class);
    private final List<String> handlerHelpMsgs;
    private transient final MasterMonitor masterMonitor;
    private static final long serialVersionUID = HelpHandlerServlet.class.hashCode();
    private static final Func1<Observable<? extends Throwable>, Observable<?>> retryLogic =
            RetryUtils.getRetryFunc(logger, 5);
    private transient final Registry registry;

    private transient final SessionContextBuilder sessionContextBuilder;

    public HelpHandlerServlet(List<String> handlerHelpMsgs, MasterMonitor masterMonitor, Registry registry, SessionContextBuilder sessionContextBuilder) {
        this.handlerHelpMsgs = handlerHelpMsgs;
        this.masterMonitor = masterMonitor;
        this.registry = registry;
        this.sessionContextBuilder = sessionContextBuilder;
    }

    @Override
    public void doOptions(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        final SessionContext httpSessionCtx = sessionContextBuilder.createHttpSessionCtx(request.getRemoteAddr(),
                request.getRequestURI() + "?" + request.getQueryString(), request.getMethod());
        CountDownLatch latch = new CountDownLatch(1);
        Subscription subscription = null;
        try {
            subscription = MantisClientUtil.callGetOnMaster(masterMonitor.getMasterObservable(), "/help", registry)
                    .retryWhen(retryLogic)
                    .doOnError(t -> {
                        logger.warn("Timed out relaying request for session id " + httpSessionCtx.getId() +
                                " url=" + request.getRequestURI());
                        response.setStatus(HttpServletResponse.SC_REQUEST_TIMEOUT);
                        try {
                            response.getOutputStream().print(t.getMessage());
                            response.flushBuffer();
                        } catch (IOException e) {
                            logger.info("session id " + httpSessionCtx.getId() + ": error writing error (" +
                                    t.getMessage() + ") to client: " + e.getMessage());
                        }
                        latch.countDown();
                    })
                    .flatMap(masterResponse -> {
                        return masterResponse.getByteBuf().map(bb -> {
                            HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
                            String mantisHelp = bb.toString(Charset.forName("UTF-8"));
                            JSONArray jsonArray = new JSONArray(mantisHelp);
                            for (String h : handlerHelpMsgs)
                                jsonArray.put(h);
                            try {
                                response.getOutputStream().print(jsonArray.toString());
                                response.flushBuffer();
                            } catch (IOException e) {
                                logger.info("session id " + httpSessionCtx.getId() +
                                        ": error writing help message to client: " + e.getMessage());
                            }
                            latch.countDown();
                            return Observable.empty();
                        });
                    })
                    .subscribe();
            while (latch.getCount() > 0) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    logger.warn("Retrying after getting interrupted waiting on latch: " + e.getMessage());
                    try {Thread.sleep(100);} catch (InterruptedException ie) {}
                }
            }
        } finally {
            if (subscription != null && !subscription.isUnsubscribed())
                subscription.unsubscribe();
            httpSessionCtx.endSession();
        }
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }
}
