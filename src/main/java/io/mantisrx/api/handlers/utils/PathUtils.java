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

public class PathUtils {

    public static String getTokenAfter(String path, String token) {
        if (path == null)
            return "";
        int i = path.indexOf(token + "/");
        if (i < 0)
            return "";
        String split = path.substring(i + token.length() + 1);
        i = split.indexOf('/');
        return i < 0 ?
                split :
                split.substring(0, i);
    }

    public static String getPathAfter(String path, String token) {
        if (path == null)
            return "";
        int i = path.indexOf(token + "/");
        if (i < 0)
            return "";
        return path.substring(i + token.length() + 1);
    }

    public static void main(String[] args) {
        System.out.println(getTokenAfter("/jobconnectbyid/sine-function-10", "/jobconnectbyid"));
    }
}
