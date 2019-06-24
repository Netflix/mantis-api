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

import java.util.Objects;


public class JobDiscoveryLookupKey {

    public enum LookupType {
        JOB_CLUSTER,
        JOB_ID
    }

    private final LookupType lookupType;
    private final String id;

    public JobDiscoveryLookupKey(final LookupType lookupType, final String id) {
        this.lookupType = lookupType;
        this.id = id;
    }

    public LookupType getLookupType() {
        return lookupType;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final JobDiscoveryLookupKey that = (JobDiscoveryLookupKey) o;
        return lookupType == that.lookupType &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {

        return Objects.hash(lookupType, id);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobDiscoveryLookupKey{");
        sb.append("lookupType=").append(lookupType);
        sb.append(", id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
