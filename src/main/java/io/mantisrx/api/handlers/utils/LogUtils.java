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

package io.mantisrx.api.handlers.utils;

import io.mantisrx.api.SessionContext;
import io.mantisrx.api.handlers.servlets.MantisAPIRequestHandler;


public class LogUtils {

    public static String getGetStartMsg(MantisAPIRequestHandler handler, SessionContext context) {
        return getStartMsg("GET", handler, context);
    }

    public static String getPostStartMsg(MantisAPIRequestHandler handler, SessionContext context) {
        return getStartMsg("POST", handler, context);
    }

    public static String getStartMsg(String method, MantisAPIRequestHandler handler, SessionContext context) {
        return handler.getName() + " " + method + " starting, id " + context.getId() + " client " +
                context.getRemoteAddress();
    }
}
