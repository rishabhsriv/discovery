/*
 * Copyright 2010 Proofpoint, Inc.
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
package com.proofpoint.discovery;

import com.proofpoint.discovery.store.DistributedStore;

import javax.inject.Inject;
import java.util.stream.Stream;

import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;
import static java.util.Objects.requireNonNull;

public class ReplicatedDynamicStore
        implements DynamicStore
{
    private final DistributedStore store;

    @Inject
    public ReplicatedDynamicStore(@ForDynamicStore DistributedStore store, DiscoveryConfig config)
    {
        this.store = requireNonNull(store, "store is null");
    }

    @Override
    public void put(Id<Node> nodeId, DynamicAnnouncement announcement)
    {
        store.put(nodeId, announcement);
    }

    @Override
    public void delete(Id<Node> nodeId)
    {
        store.delete(nodeId);
    }

    @Override
    public Stream<Service> getAll()
    {
        return store.getAll();
    }

    @Override
    public Stream<Service> get(String type)
    {
        return getAll()
                .filter(matchesType(type));
    }

    @Override
    public Stream<Service> get(String type, String pool)
    {
        return getAll()
                .filter(matchesType(type).and(matchesPool(pool)));
    }
}
