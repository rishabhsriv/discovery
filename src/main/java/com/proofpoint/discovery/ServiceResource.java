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

import com.google.inject.Inject;
import com.proofpoint.node.NodeInfo;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.proofpoint.discovery.Services.services;

@Path("/v1/service")
public class ServiceResource
{
    private final DynamicStore dynamicStore;
    private final ConfigStore configStore;
    private final ProxyStore proxyStore;
    private final NodeInfo node;
    private final InitializationTracker initializationTracker;

    @Inject
    public ServiceResource(DynamicStore dynamicStore, ConfigStore configStore, ProxyStore proxyStore, NodeInfo node,
            InitializationTracker initializationTracker)
    {
        this.dynamicStore = dynamicStore;
        this.configStore = configStore;
        this.proxyStore = proxyStore;
        this.node = node;
        this.initializationTracker = initializationTracker;
    }

    @GET
    @Path("{type}/{pool}")
    @Produces(MediaType.APPLICATION_JSON)
    public Services getServices(@PathParam("type") String type, @PathParam("pool") String pool)
    {
        ensureInitialized();
        return services(node.getEnvironment(), firstNonNull(proxyStore.get(type, pool),
                Stream.concat(configStore.get(type, pool), dynamicStore.get(type, pool))));
    }

    @GET
    @Path("{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public Services getTypeServices(@PathParam("type") String type)
    {
        ensureInitialized();
        return services(node.getEnvironment(), firstNonNull(proxyStore.get(type),
                Stream.concat(configStore.get(type), dynamicStore.get(type))));
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Services getAllServices()
    {
        ensureInitialized();
        Iterable<Service> services = Stream.concat(configStore.getAll(), dynamicStore.getAll()).collect(Collectors.toList());
        return services(node.getEnvironment(), proxyStore.filterAndGetAll(services));
    }

    private void ensureInitialized()
    {
        if (initializationTracker.isPending()) {
            throw new WebApplicationException(503);
        }
    }
}
