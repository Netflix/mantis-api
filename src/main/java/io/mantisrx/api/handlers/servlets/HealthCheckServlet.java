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

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.PropertyNames;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.WorkerThreadPool;
import io.mantisrx.api.handlers.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HealthCheckServlet extends HttpServlet {

    private static Logger logger = LoggerFactory.getLogger(HealthCheckServlet.class);
    private static final long serialVersionUID = HealthCheckServlet.class.hashCode();
    private transient final Property<Long> MIN_HEAP_THRESH_BYTES;
    private transient final WorkerThreadPool workerThreadPool;
    private transient final Registry registry;
    private transient final PropertyRepository propertyRepository;

    public HealthCheckServlet(PropertyRepository propertyRepository, Registry registry, WorkerThreadPool workerThreadPool) {
        this.workerThreadPool = workerThreadPool;
        this.registry = registry;
        this.propertyRepository = propertyRepository;

        MIN_HEAP_THRESH_BYTES = propertyRepository.get(PropertyNames.healthCheckMinHeapBytes, Long.class).orElse((long) (1.5 * 1024 * 1024 * 1024));
    }


    @Override
    public void doOptions(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        final SessionContextBuilder contextBuilder = SessionContextBuilder.getInstance(propertyRepository, registry, workerThreadPool);
        final SessionContext httpSessionCtx = contextBuilder.createHttpSessionCtx(request.getRemoteAddr(),
                request.getRequestURI() + "?" + request.getQueryString(), request.getMethod());
        try {
            final Runtime runtime = Runtime.getRuntime();
            long total = runtime.totalMemory();
            long max = runtime.maxMemory();
            long free = runtime.freeMemory();
            if ((max - total + free) < MIN_HEAP_THRESH_BYTES.get()) {
                logger.error("Too little memory left, failing health check: max: " + max + ", total: " + total + ", free: " + free);
                HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                return;
            }
            doSimpleHealthCheck(response, request);
        } finally {
            httpSessionCtx.endSession();
        }
    }

    private void doSimpleHealthCheck(HttpServletResponse response, HttpServletRequest request) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_OK);
    }
}
