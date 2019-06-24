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

package io.mantisrx.api.filters;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.mantisrx.api.SpectatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricsFilter implements Filter {

    private static Logger logger = LoggerFactory.getLogger(MetricsFilter.class);
    private Counter mantisApiStatus;
    private final Registry registry;
    private final ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>(10);

    public MetricsFilter(Registry registry) {
        this.registry = registry;
    }

    public void init(FilterConfig filterConfig) {
        mantisApiStatus = SpectatorUtils.buildAndRegisterCounter(registry, "mantisapi.status");
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        doFilter((HttpServletRequest) request, (HttpServletResponse) response, chain);
    }

    public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws IOException, ServletException {
        String status = String.valueOf(response.getStatus());
        counters.computeIfAbsent(status,
                s -> SpectatorUtils.buildAndRegisterCounter(registry, "mantisapi.status", "code", s)).increment();
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
}

