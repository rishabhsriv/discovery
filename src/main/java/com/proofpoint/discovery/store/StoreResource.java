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
package com.proofpoint.discovery.store;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.proofpoint.discovery.AuthManager;
import com.proofpoint.discovery.DiscoveryConfig;
import com.proofpoint.discovery.DiscoveryConfig.ReplicationMode;
import com.proofpoint.units.Duration;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;

@Path("/v1/store/{store}")
public class StoreResource
{
    private final Map<String, InMemoryStore> localStores;
    private final Map<String, Duration> tombstoneMaxAges;
    private final String generalPoolMapTarget;
    private final ReplicationMode generalPoolLegacyReplicationMode;
    private final AuthManager authManager;

    @Inject
    public StoreResource(Map<String, InMemoryStore> localStores, Map<String, StoreConfig> configs, DiscoveryConfig discoveryConfig, AuthManager authManager)
    {
        this.localStores = ImmutableMap.copyOf(localStores);
        this.tombstoneMaxAges = ImmutableMap.copyOf(Maps.transformValues(configs, StoreConfig::getTombstoneMaxAge));
        generalPoolMapTarget = discoveryConfig.getGeneralPoolMapTarget();
        generalPoolLegacyReplicationMode = discoveryConfig.getGeneralPoolLegacyReplicationMode();
        this.authManager = authManager;
    }

    @POST
    @Consumes({"application/x-jackson-smile", "application/json"})
    public Response setMultipleEntries(@PathParam("store") String storeName, List<Entry> entries, @Context HttpServletRequest request)
    {
        authManager.checkAuthReplicate(request);
        InMemoryStore store = localStores.get(storeName);
        Duration tombstoneMaxAge = tombstoneMaxAges.get(storeName);
        if (store == null || tombstoneMaxAge == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        for (Entry entry : entries) {
            if (!isExpired(tombstoneMaxAge, entry)) {
                if (generalPoolLegacyReplicationMode != ReplicationMode.PHASE_THREE) {
                    entry = Entries.transformPools(entry, "general", generalPoolMapTarget);
                }
                store.put(entry);
            }
        }
        return Response.noContent().build();
    }

    @GET
    @Produces({"application/x-jackson-smile", "application/json"})
    public Response getAll(@PathParam("store") String storeName, @Context HttpServletRequest request)
    {
        authManager.checkAuthRead(request);
        InMemoryStore store = localStores.get(storeName);
        if (store == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        if (generalPoolLegacyReplicationMode == ReplicationMode.PHASE_ONE) {
            return Response.ok(
                    store.getAll().stream()
                            .map(entry -> Entries.transformPools(entry, generalPoolMapTarget, "general"))
                            .collect(toImmutableList()))
                    .build();
        }

        return Response.ok(store.getAll()).build();
    }

    private static boolean isExpired(Duration tombstoneMaxAge, Entry entry)
    {
        long ageInMs = System.currentTimeMillis() - entry.getTimestamp();

        return (entry.getValue() == null && ageInMs > tombstoneMaxAge.toMillis()) ||
                (entry.getMaxAgeInMs() != null && ageInMs > entry.getMaxAgeInMs());
    }
}
