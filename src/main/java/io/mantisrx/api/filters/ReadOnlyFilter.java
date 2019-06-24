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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import io.mantisrx.api.PropertyNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReadOnlyFilter implements Filter {

    private static Logger logger = LoggerFactory.getLogger(ReadOnlyFilter.class);
    private ServletContext _context;

    private static final String READ_ONLY_PROP_NAME = "mantisapi.readonly";
    private final PropertyRepository propertyRepository;

    public ReadOnlyFilter(PropertyRepository propertyRepository) {
        this.propertyRepository = propertyRepository;
    }

    /* ------------------------------------------------------------ */
    /*
     * @see javax.servlet.Filter#init(javax.servlet.FilterConfig)
     */
    public void init(FilterConfig filterConfig) {
        _context = filterConfig.getServletContext();
    }

    /* ------------------------------------------------------------ */
    /*
     * @see javax.servlet.Filter#doFilter(javax.servlet.ServletRequest, javax.servlet.ServletResponse, javax.servlet.FilterChain)
     */
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        Property<Boolean> isCurrentlyReadOnly = propertyRepository.get(PropertyNames.mantisAPIReadOnly, Boolean.class).orElse(false);

        if (isCurrentlyReadOnly.get()) {
            try {
                HttpServletRequest req = (HttpServletRequest) request;
                if (!(req.getMethod().equalsIgnoreCase("GET") || req.getMethod().equalsIgnoreCase("OPTIONS"))) {
                    HttpServletResponse resp = ((HttpServletResponse) response);
                    resp.setStatus(403);
                    resp.getWriter().write("{\"error\": \"MantisAPI is currently Read Only via " + READ_ONLY_PROP_NAME + "property.\"}");
                    resp.getWriter().flush();
                    resp.flushBuffer();
                    return;
                } else {
                    chain.doFilter(request, response);
                }
            } catch (Exception ex) {
                logger.error("Error during ReadOnly mode: {}", ex.getMessage());
            }

        } else {
            chain.doFilter(request, response);
        }
    }

    /* ------------------------------------------------------------ */
    /*
     * @see javax.servlet.Filter#destroy()
     */
    public void destroy() {
    }

}
