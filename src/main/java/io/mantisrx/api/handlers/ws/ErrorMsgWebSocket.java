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

package io.mantisrx.api.handlers.ws;

import java.io.IOException;

import io.mantisrx.api.SessionContext;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ErrorMsgWebSocket extends WebSocketAdapter {

    private static Logger logger = LoggerFactory.getLogger(ErrorMsgWebSocket.class);
    private final String errorMessage;
    private final SessionContext sessionContext;

    public ErrorMsgWebSocket(String errorMessage, SessionContext sessionContext) {
        this.errorMessage = errorMessage;
        this.sessionContext = sessionContext;
    }

    @Override
    public void onWebSocketConnect(Session sess) {
        super.onWebSocketConnect(sess);
        try {
            sess.getRemote().sendString(errorMessage);
        } catch (IOException e) {
            logger.warn(String.format("Couldn't send error message (%s) to client session %d: %s", errorMessage,
                    sessionContext.getId(), e.getMessage()));
        }
        sess.close();
    }
}
