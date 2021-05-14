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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.proofpoint.bootstrap.LifeCycleManager;
import com.proofpoint.discovery.AllowAllAuthManager;
import com.proofpoint.discovery.AuthManager;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceState;
import com.proofpoint.discovery.client.testing.StaticServiceSelector;
import com.proofpoint.http.server.testing.TestingHttpServer;
import com.proofpoint.http.server.testing.TestingHttpServerModule;
import com.proofpoint.json.JsonModule;
import com.proofpoint.node.testing.TestingNodeModule;
import com.proofpoint.reporting.ReportingModule;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.proofpoint.bootstrap.Bootstrap.bootstrapTest;
import static com.proofpoint.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.proofpoint.jaxrs.JaxrsModule.explicitJaxrsModule;

public class TestingStoreServer
{
    private final LifeCycleManager lifeCycleManager;
    private final InMemoryStore inMemoryStore = new InMemoryStore();
    private final AuthManager authManager = new AllowAllAuthManager();
    private final AtomicBoolean serverInSelector = new AtomicBoolean(true);
    private final ServiceSelector serviceSelector;

    public TestingStoreServer(StoreConfig storeConfig)
    {
        Injector injector;
        try {
            injector = bootstrapTest()
                    .withModules(
                            new TestingNodeModule(),
                            new TestingHttpServerModule(),
                            explicitJaxrsModule(),
                            new JsonModule(),
                            new ReportingModule(),
                            binder -> {
                                binder.bind(StoreConfig.class).toInstance(storeConfig);
                                binder.bind(AuthManager.class).toInstance(authManager);
                                jaxrsBinder(binder).bind(StoreResource.class);
                                binder.bind(new TypeLiteral<Map<String, InMemoryStore>>() {})
                                        .toInstance(ImmutableMap.of("dynamic", inMemoryStore));
                                binder.bind(new TypeLiteral<Map<String, StoreConfig>>() {})
                                        .toInstance(ImmutableMap.of("dynamic", storeConfig));
                            }
                    )
                    .initialize();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        serviceSelector = new SwitchableServiceSelector(new StaticServiceSelector(new ServiceDescriptor(
                UUID.randomUUID(),
                UUID.randomUUID().toString(),
                "discovery",
                "general",
                "/location",
                ServiceState.RUNNING,
                ImmutableMap.of("http", injector.getInstance(TestingHttpServer.class).getBaseUrl().toString())
        )));
    }

    public InMemoryStore getInMemoryStore()
    {
        return inMemoryStore;
    }

    public void reset()
    {
        serverInSelector.set(true);
        for (Entry entry : inMemoryStore.getAll()) {
            inMemoryStore.delete(entry.getKey(), entry.getTimestamp());
        }
    }

    public void stop()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public void setServerInSelector(boolean serverInSelector)
    {
        this.serverInSelector.set(serverInSelector);
    }

    public ServiceSelector getServiceSelector()
    {
        return serviceSelector;
    }

    private class SwitchableServiceSelector
        implements ServiceSelector
    {
        private final ServiceSelector delegate;

        SwitchableServiceSelector(ServiceSelector delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public String getType()
        {
            return delegate.getType();
        }

        @Override
        public String getPool()
        {
            return delegate.getPool();
        }

        @Override
        public List<ServiceDescriptor> selectAllServices()
        {
            if (serverInSelector.get()) {
                return delegate.selectAllServices();
            }
            return ImmutableList.of();
        }
    }
}
