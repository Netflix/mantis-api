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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import com.netflix.spectator.api.Tag;


public class QueryParams {

    public static final String TunnelPingParamName = "MantisApiTunnelPingEnabled";
    public static final String TagsParamName = "MantisApiTag";
    public static final String OriginRegionTagName = "originRegion";
    public static final String TagNameValDelimiter = ":";

    public static List<Tag> getTaglist(Map<String, List<String>> queryParameters, long id, String url) {
        final List<Tag> tagList = new ArrayList<>();

        if (queryParameters != null) {
            final List<String> tagVals = queryParameters.get(QueryParams.TagsParamName);
            if (tagVals != null) {
                for (String s : tagVals) {
                    StringTokenizer tokenizer = new StringTokenizer(s, QueryParams.TagNameValDelimiter);
                    if (tokenizer.countTokens() == 2) {
                        String s1 = tokenizer.nextToken();
                        String s2 = tokenizer.nextToken();
                        if (s1 != null && !s1.isEmpty() && s2 != null && !s2.isEmpty()) {
                            tagList.add(Tag.of(s1, s2));
                        }
                    }
                }
            }
        }

        tagList.add(Tag.of("SessionId", String.valueOf(id)));
        tagList.add(Tag.of("urlPath", HttpUtils.getUrlMinusQueryStr(url)));
        return tagList;
    }

    public static String getTunnelConnectParams() {
        return QueryParams.TunnelPingParamName + "=true&" +
                QueryParams.TagsParamName + "=" + QueryParams.OriginRegionTagName + QueryParams.TagNameValDelimiter +
                Regions.getLocalRegion();
    }

}
