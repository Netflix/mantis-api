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

import io.mantisrx.api.handlers.ServletConx;
import org.eclipse.jetty.websocket.api.Session;


public class WebsocketUtils {

    public static ServletConx getWSConx(final Session connection) {
        return new ServletConx() {
            @Override
            public void sendMessage(String s) throws IOException {
                if (!connection.isOpen())
                    throw new IOException("Conx closed");
                connection.getRemote().sendString(s);
            }

            @Override
            public void close() {
                connection.close();
            }

            @Override
            public boolean isOpen() {
                return connection.isOpen();
            }
        };
    }
}
