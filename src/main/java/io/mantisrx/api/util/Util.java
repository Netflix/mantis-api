/**
 * Copyright 2018 Netflix, Inc.
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
package io.mantisrx.api.util;

import io.netty.handler.codec.http.QueryStringDecoder;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@UtilityClass
@Slf4j
public class Util {
    public static String getTokenAfter(String path, String suffix) {
        if (path == null)
            return "";
        int i = path.indexOf(suffix + "/");
        if (i < 0)
            return "";
        String split = path.substring(i + suffix.length() + 1);
        i = split.indexOf('/');
        return i < 0 ?
                split :
                split.substring(0, i);
    }


    public static boolean startsWithAnyOf(final String target, List<String> prefixes) {
        for (String prefix : prefixes) {
            if (target.startsWith(prefix)) {
                return  true;
            }
        }
        return false;
    }

    //
    // Regions
    //

    public static String getLocalRegion() {
        return System.getenv("EC2_REGION");
    }

    //
    // Query Params
    //

    public static final String TunnelPingParamName = "MantisApiTunnelPingEnabled";
    public static final String OriginRegionTagName = "originRegion";
    public static final String TagsParamName = "MantisApiTag";
    public static final String TagNameValDelimiter = ":";

    public static String[] getTaglist(String uri, String id) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
        Map<String, List<String>> queryParameters = queryStringDecoder.parameters();


        final List<String> tags = new LinkedList<>();
        if (queryParameters != null) {
            final List<String> tagVals = queryParameters.get(TagsParamName);
            if (tagVals != null) {
                for (String s : tagVals) {
                    StringTokenizer tokenizer = new StringTokenizer(s, TagNameValDelimiter);
                    if (tokenizer.countTokens() == 2) {
                        String s1 = tokenizer.nextToken();
                        String s2 = tokenizer.nextToken();
                        if (s1 != null && !s1.isEmpty() && s2 != null && !s2.isEmpty()) {
                            tags.add(s1);
                            tags.add(s2);
                        }
                    }
                }
            }
        }

        tags.add("SessionId");
        tags.add(id);

        tags.add("urlPath");
        tags.add(queryStringDecoder.path());

        log.info(tags.toString());

        return tags.toArray(new String[]{});
    }

    public static String getTunnelConnectParams() {
        return TunnelPingParamName + "=true&" +
                TagsParamName + "=" + OriginRegionTagName + TagNameValDelimiter +
                getLocalRegion();
    }
}
