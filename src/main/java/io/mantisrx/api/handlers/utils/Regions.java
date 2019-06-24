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
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.netflix.archaius.api.PropertyRepository;
import io.mantisrx.api.PropertyNames;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Regions {

    private static final Logger logger = LoggerFactory.getLogger(Regions.class);
    private final static String ALL = "ALL";
    private final static String REMOTE = "REMOTE";
    private static String localRegion = Strings.isNullOrEmpty(System.getenv("EC2_REGION")) ? "Unknown" : System.getenv("EC2_REGION");
    private final static String regKey = "mantis.meta.origin";
    private final static String errKey = "mantis.meta.errorString";
    private final static String codeKey = "mantis.meta.origin.response.code";

    private static List<String> getRegionsFromProperty(String property) {
        List<String> rlist = new ArrayList<>();
        StringTokenizer tokenizer = new StringTokenizer(property, ",");
        while (tokenizer.hasMoreTokens())
            rlist.add(tokenizer.nextToken());
        return Collections.unmodifiableList(rlist);
    }

    public static List<String> getMatchingRegions(PropertyRepository propertyRepository, String match) throws InvalidRegionException {
        String property = propertyRepository.get(PropertyNames.mantisAPIRegions, String.class).orElse("us-east-1,us-west-2,eu-west-1").get();
        List<String> regions = getRegionsFromProperty(property);

        if (ALL.equalsIgnoreCase(match))
            return regions;
        List<String> result = new ArrayList<>();
        if (REMOTE.equalsIgnoreCase(match)) {
            for (String r : regions)
                if (!isLocalRegion(r))
                    result.add(r);
        } else {
            for (String r : regions) {
                if (r.matches(match))
                    result.add(r);
            }
        }
        if (result.isEmpty()) {
            throw new InvalidRegionException(match);
        }
        return result;
    }

    public static boolean isLocalRegion(String region) {
        Preconditions.checkNotNull(region, "region was null");
        return region.equals(getLocalRegion());
    }

    public static String getLocalRegion() {
        return localRegion;
    }

    public static String getWrappedJson(String data, String region, String err) {
        return getWrappedJsonIntl(data, region, err, 0, false);
    }

    public static String getForceWrappedJson(String data, String region, int code, String err) {
        return getWrappedJsonIntl(data, region, err, code, true);
    }

    private static String getWrappedJsonIntl(String data, String region, String err, int code, boolean forceJson) {
        try {
            JSONObject o = new JSONObject(data);
            o.put(regKey, region);
            if (err != null && !err.isEmpty())
                o.put(errKey, err);
            if (code > 0)
                o.put(codeKey, "" + code);
            return o.toString();
        } catch (JSONException e) {
            try {
                JSONArray a = new JSONArray(data);
                if (!forceJson)
                    return data;
                JSONObject o = new JSONObject();
                o.put(regKey, region);
                if (err != null && !err.isEmpty())
                    o.put(errKey, err);
                if (code > 0)
                    o.put(codeKey, "" + code);
                o.accumulate("response", a);
                return o.toString();
            } catch (JSONException ae) {
                if (!forceJson)
                    return data;
                JSONObject o = new JSONObject();
                o.put(regKey, region);
                if (err != null && !err.isEmpty())
                    o.put(errKey, err);
                if (code > 0)
                    o.put(codeKey, "" + code);
                o.put("response", data);
                return o.toString();
            }
        }
    }

}
