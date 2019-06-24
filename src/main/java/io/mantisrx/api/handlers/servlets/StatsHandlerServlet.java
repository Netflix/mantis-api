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
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.api.SessionContext;
import io.mantisrx.api.SessionContextBuilder;
import io.mantisrx.api.handlers.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StatsHandlerServlet extends HttpServlet {

    private static Logger logger = LoggerFactory.getLogger(StatsHandlerServlet.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final String endpointName = "sessionstats";
    public static final String helpMsg = endpointName;
    private static final long serialVersionUID = StatsHandlerServlet.class.hashCode();
    private transient final SessionContextBuilder sessionContextBuilder;

    public StatsHandlerServlet(SessionContextBuilder sessionContextBuilder) {
        this.sessionContextBuilder = sessionContextBuilder;
    }

    @Override
    public void doOptions(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        final Map<Long, SessionContext> sessionContextMap = sessionContextBuilder.getActiveSessions();
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_OK);
        try {
            response.getOutputStream().print(objectMapper.writeValueAsString(sessionContextMap));
            response.flushBuffer();
        } catch (IOException e) {
            logger.info("Error writing stats to client: " + e.getMessage());
        }
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) {
        HttpUtils.addBaseHeaders(response, "GET", "OPTIONS");
        response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }
}
