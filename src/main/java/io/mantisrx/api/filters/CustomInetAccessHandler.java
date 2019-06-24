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
import java.net.InetSocketAddress;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.InetAccessHandler;


public class CustomInetAccessHandler extends InetAccessHandler {

    /**
     * Checks the incoming request against the whitelist and blacklist
     * <p>
     * We override this method from the InetAccessHandler to get the forwarded IP address in the event that an ELB
     * has handled this request. The original handler always uses real IP as forwarded headers can be faked.
     * We assume we're not dealing with a bad actor internally, but instead a misconfigured script and thus header
     * faking should not be an issue.
     */
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        HttpChannel channel = baseRequest.getHttpChannel();
        if (channel != null) {
            EndPoint endp = channel.getEndPoint();
            if (endp != null) {
                InetSocketAddress address = new InetSocketAddress(request.getRemoteAddr(), request.getRemotePort());
                if (address != null && !isAllowed(address.getAddress(), request)) {
                    response.sendError(HttpStatus.FORBIDDEN_403);
                    baseRequest.setHandled(true);
                    return;
                }
            }
        }

        getHandler().handle(target, baseRequest, request, response);
    }
}
