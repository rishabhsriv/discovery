/*
 * Copyright 2017 Proofpoint, Inc.
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
package com.proofpoint.discovery.store;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.proofpoint.discovery.Service;

import java.util.List;

import static com.proofpoint.discovery.store.Entry.entry;

class Entries
{
    private Entries()
    {
    }

    static Entry transformPools(Entry entry, String fromPool, String toPool)
    {
        List<Service> services = entry.getValue();
        if (services != null) {
            boolean transformed = false;
            Builder<Service> builder = ImmutableList.builder();
            for (Service service : services) {
                if (service.getPool().equals(fromPool)) {
                    service = Service.copyOf(service).setPool(toPool).build();
                    transformed = true;
                }
                builder.add(service);
            }
            if (transformed) {
                entry = entry(entry.getKey(), builder.build(), entry.getTimestamp(), entry.getMaxAgeInMs(), entry.getAnnouncer());
            }
        }
        return entry;
    }
}
