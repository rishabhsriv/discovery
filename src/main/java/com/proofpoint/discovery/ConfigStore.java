/*
 * Copyright 2016 Proofpoint, Inc.
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

import com.google.auto.value.AutoValue;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ConfigStore
{
    private final Table<String, String, Set<Service>> table;

    @Inject
    public ConfigStore(final ConfigStoreConfig config)
    {
        Multimap<TypeAndPool, Service> multimap = HashMultimap.create();
        for (Entry<String, StaticAnnouncementConfig> entry : config.getAnnouncements().entrySet()) {
            Service service = new Service(
                    Id.valueOf(UUID.nameUUIDFromBytes(entry.getKey().getBytes(UTF_8))),
                    null,
                    entry.getValue().getType(),
                    entry.getValue().getPool(),
                    "/somewhere/" + entry.getKey(),
                    entry.getValue().getProperties());
            multimap.put(new AutoValue_ConfigStore_TypeAndPool(entry.getValue().getType(), entry.getValue().getPool()), service);
        }

        ImmutableTable.Builder<String, String, Set<Service>> builder = ImmutableTable.builder();
        for (Entry<TypeAndPool, Collection<Service>> entry : multimap.asMap().entrySet()) {
            builder.put(entry.getKey().getType(), entry.getKey().getPool(), ImmutableSet.copyOf(entry.getValue()));
        }

        table = builder.build();
    }

    public Set<Service> getAll()
    {
        Builder<Service> builder = ImmutableSet.builder();
        table.values().forEach(builder::addAll);
        return builder.build();
    }

    @Nullable
    public Set<Service> get(String type)
    {
        Builder<Service> builder = ImmutableSet.builder();
        table.row(type).values().forEach(builder::addAll);
        return builder.build();
    }

    @Nullable
    public Set<Service> get(String type, final String pool)
    {
        return firstNonNull(table.get(type, pool), ImmutableSet.<Service>of());
    }

    @AutoValue
    abstract static class TypeAndPool
    {
        abstract String getType();

        abstract String getPool();
    }
}
