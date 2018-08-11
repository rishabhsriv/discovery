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
import com.proofpoint.json.JsonCodec;
import com.proofpoint.units.Duration;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.proofpoint.discovery.DynamicServiceAnnouncement.toServiceWith;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;
import static java.util.Objects.requireNonNull;

public class ReplicatedDynamicStore
        implements DynamicStore
{
    private final JsonCodec<List<Service>> codec = JsonCodec.listJsonCodec(Service.class);

    private final DistributedStore store;
    private final Duration maxAge;

    @Inject
    public ReplicatedDynamicStore(@ForDynamicStore DistributedStore store, DiscoveryConfig config)
    {
        this.store = requireNonNull(store, "store is null");
        this.maxAge = Objects.requireNonNull(config, "config is null").getMaxAge();
    }

    @Override
    public void put(Id<Node> nodeId, DynamicAnnouncement announcement)
    {
        List<Service> services = announcement.getServiceAnnouncements().stream()
                .map(toServiceWith(nodeId, announcement.getLocation(), announcement.getPool()))
                .collect(Collectors.toList());

        byte[] key = nodeId.getBytes();
        byte[] value = codec.toJsonBytes(services);

        store.put(key, value, maxAge);
    }

    @Override
    public void delete(Id<Node> nodeId)
    {
        store.delete(nodeId.getBytes());
    }

    @Override
    public Collection<Service> getAll()
    {
        return store.getAll()
                .map(entry -> codec.fromJson(entry.getValue()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public Stream<Service> get(String type)
    {
        return getAll().stream()
                .filter(matchesType(type));
    }

    @Override
    public Stream<Service> get(String type, String pool)
    {
        return getAll().stream()
                .filter(matchesType(type).and(matchesPool(pool)));
    }
}
