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

package io.mantisrx.api.handlers.domain;

import java.io.IOException;

import org.eclipse.jetty.servlets.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MantisEventSource implements EventSource {

    private static final Logger logger = LoggerFactory.getLogger(MantisEventSource.class);
    private Emitter emitter;

    public void onOpen(Emitter emitter) throws IOException {
        this.emitter = emitter;
    }

    public void emitEvent(String dataToSend) throws IOException {
        if (emitter != null) {
            this.emitter.data(dataToSend);
        } else {
            logger.error("(Emitter is null) FAILED to send data {}", dataToSend);
            throw new IOException(new IllegalStateException("emitter is null when sending data"));
        }
    }

    public void onClose() {
        if (emitter != null) {
            this.emitter.close();
        }
    }
}
