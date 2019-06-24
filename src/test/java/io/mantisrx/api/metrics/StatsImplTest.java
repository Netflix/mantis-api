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

package io.mantisrx.api.metrics;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;


public class StatsImplTest {

    @Test
    public void testConstruction() {
        String json = "{\"totalNumMessages\":0,\"totalDroppedNumMessages\":0,\"totalNumBytes\":0," +
                "\"totalDroppedNumBytes\":0,\"messagesPerSec\":0.0,\"messageBitsPerSec\":0.0," +
                "\"lastDataSentAt\":1457736485907}";
        ObjectMapper mapper = new ObjectMapper();
        try {
            StatsImpl s = mapper.readValue(json, StatsImpl.class);
            Assert.assertTrue(true);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            Assert.fail(e.getMessage());
        }
    }
}
