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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.handlers.domain.MantisEventSource;
import org.eclipse.jetty.servlets.EventSource;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;


/**
 * SSEWebSocketServletBase's objective is to allow us to implement a single servlet
 * which can both upgrade to a WebSocket connection on demand, and also serve the same content
 * over EventSource / ServerSentEvents when no WebSocket upgrade is requested.
 * <p>
 * The majority of the body below has been copied from @see org.eclipse.jetty.servlets.EventSourceServlet
 * with a few notable changes;
 * <p>
 * 1) The #init() method calls super.init() to allow WebSocketServlet to initialize itself.
 * 2) We've removed the check for Accept: text/event-stream headers because we did not require it in
 * previous versions and want to maintain backwards compatibility.
 */
public abstract class SSEWebSocketServletBase extends WebSocketServlet {

    protected EventSource newEventSource(HttpServletRequest request, HttpServletResponse response) {
        return new MantisEventSource();
    }

    private final Property<Long> websocketIdleTimeout;

    public SSEWebSocketServletBase(PropertyRepository propertyRepository) {
        websocketIdleTimeout = propertyRepository.get(PropertyNames.websocketTimeout, Long.class).orElse(300000L);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        EventSource eventSource = newEventSource(request, response);
        if (eventSource == null) {
            response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        } else {
            respond(request, response);
            AsyncContext async = request.startAsync();
            // Infinite timeout because the continuation is never resumed,
            // but only completed on close
            async.setTimeout(0);
            SSEWebSocketServletBase.EventSourceEmitter emitter = new SSEWebSocketServletBase.EventSourceEmitter(eventSource, async);
            emitter.scheduleHeartBeat();
            open(eventSource, emitter);
        }
    }

    @Override
    public void configure(WebSocketServletFactory factory) {
        factory.getPolicy().setIdleTimeout(websocketIdleTimeout.get());
    }


    //
    // SSE
    //


    private static final byte[] CRLF = new byte[] {'\r', '\n'};
    private static final byte[] EVENT_FIELD = "event: ".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DATA_FIELD = "data: ".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMMENT_FIELD = ": ".getBytes(StandardCharsets.UTF_8);

    private ScheduledExecutorService scheduler;
    private int heartBeatPeriod = 10;

    @Override
    public void init() throws ServletException {
        super.init();
        String heartBeatPeriodParam = getServletConfig().getInitParameter("heartBeatPeriod");
        if (heartBeatPeriodParam != null)
            heartBeatPeriod = Integer.parseInt(heartBeatPeriodParam);
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void destroy() {
        if (scheduler != null)
            scheduler.shutdown();
    }


    protected void respond(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setContentType("text/event-stream");
        // By adding this header, and not closing the connection,
        // we disable HTTP chunking, and we can use write()+flush()
        // to send data in the text/event-stream protocol
        response.addHeader("Connection", "close");
        response.flushBuffer();
    }

    protected void open(EventSource eventSource, EventSource.Emitter emitter) throws IOException {
        eventSource.onOpen(emitter);
    }

    public class EventSourceEmitter implements EventSource.Emitter, Runnable {

        private final EventSource eventSource;
        private final AsyncContext async;
        private final ServletOutputStream output;
        private Future<?> heartBeat;
        private boolean closed;

        public EventSourceEmitter(EventSource eventSource, AsyncContext async) throws IOException {
            this.eventSource = eventSource;
            this.async = async;
            this.output = async.getResponse().getOutputStream();
        }

        @Override
        public void event(String name, String data) throws IOException {
            synchronized (this) {
                output.write(EVENT_FIELD);
                output.write(name.getBytes(StandardCharsets.UTF_8));
                output.write(CRLF);
                data(data);
            }
        }

        @Override
        public void data(String data) throws IOException {
            synchronized (this) {
                BufferedReader reader = new BufferedReader(new StringReader(data));
                String line;
                while ((line = reader.readLine()) != null) {
                    output.write(DATA_FIELD);
                    output.write(line.getBytes(StandardCharsets.UTF_8));
                    output.write(CRLF);
                }
                output.write(CRLF);
                flush();
            }
        }

        @Override
        public void comment(String comment) throws IOException {
            synchronized (this) {
                output.write(COMMENT_FIELD);
                output.write(comment.getBytes(StandardCharsets.UTF_8));
                output.write(CRLF);
                output.write(CRLF);
                flush();
            }
        }

        @Override
        public void run() {
            // If the other peer closes the connection, the first
            // flush() should generate a TCP reset that is detected
            // on the second flush()
            try {
                synchronized (this) {
                    output.write('\r');
                    flush();
                    output.write('\n');
                    flush();
                }
                // We could write, reschedule heartbeat
                scheduleHeartBeat();
            } catch (IOException x) {
                // The other peer closed the connection
                close();
                eventSource.onClose();
            }
        }

        protected void flush() throws IOException {
            async.getResponse().flushBuffer();
        }

        @Override
        public void close() {
            synchronized (this) {
                closed = true;
                heartBeat.cancel(false);
            }
            async.complete();
        }

        protected void scheduleHeartBeat() {
            synchronized (this) {
                if (!closed)
                    heartBeat = scheduler.schedule(this, heartBeatPeriod, TimeUnit.SECONDS);
            }
        }
    }
}
