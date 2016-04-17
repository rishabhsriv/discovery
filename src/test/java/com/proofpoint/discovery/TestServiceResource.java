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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.proofpoint.bootstrap.Bootstrap;
import com.proofpoint.bootstrap.LifeCycleManager;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.StatusResponseHandler.StatusResponse;
import com.proofpoint.http.client.jetty.JettyHttpClient;
import com.proofpoint.http.server.testing.TestingHttpServer;
import com.proofpoint.http.server.testing.TestingHttpServerModule;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.json.JsonModule;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.node.testing.TestingNodeModule;
import com.proofpoint.reporting.ReportingModule;
import com.proofpoint.testing.Closeables;
import org.mockito.Mock;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.weakref.jmx.testing.TestingMBeanModule;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Sets.union;
import static com.proofpoint.bootstrap.Bootstrap.bootstrapApplication;
import static com.proofpoint.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.proofpoint.http.client.Request.Builder.prepareGet;
import static com.proofpoint.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.proofpoint.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.proofpoint.jaxrs.JaxrsModule.explicitJaxrsModule;
import static com.proofpoint.json.JsonCodec.mapJsonCodec;
import static com.proofpoint.testing.Assertions.assertEqualsIgnoreOrder;
import static javax.ws.rs.core.Response.Status.OK;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class TestServiceResource
{
    private final HttpClient client = new JettyHttpClient();
    private final JsonCodec<Map<String, Object>> mapCodec = mapJsonCodec(String.class, Object.class);

    private LifeCycleManager lifeCycleManager;
    private TestingHttpServer server;

    private InMemoryDynamicStore dynamicStore;
    @Mock
    private ProxyStore proxyStore;
    @Mock
    private InitializationTracker initializationTracker;


    @BeforeMethod
    public void setup()
            throws Exception
    {
        initMocks(this);
        dynamicStore = new InMemoryDynamicStore(new DiscoveryConfig(), new TestingTimeSupplier());
        ServiceResource resource = new ServiceResource(dynamicStore, new InMemoryStaticStore(), proxyStore, new NodeInfo("testing"), initializationTracker);

        Bootstrap app = bootstrapApplication("test-application")
                .doNotInitializeLogging()
                .withModules(
                        new TestingNodeModule(),
                        new TestingHttpServerModule(),
                        new JsonModule(),
                        explicitJaxrsModule(),
                        new ReportingModule(),
                        new TestingMBeanModule(),
                        binder -> jaxrsBinder(binder).bindInstance(resource)
                )
                .quiet();

        Injector injector = app
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        server = injector.getInstance(TestingHttpServer.class);
    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        if (lifeCycleManager != null) {
            lifeCycleManager.stop();
        }
    }

    @AfterClass(alwaysRun = true)
    public void teardownClass()
    {
        Closeables.closeQuietly(client);
    }

    @Test
    public void testGetByType()
    {
        Id<Node> redNodeId = Id.random();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(Id.<Service>random() , "storage", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", of(redStorage, redWeb));

        Id<Node> greenNodeId = Id.random();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "alpha", "/x/y/z", of(greenStorage));

        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "beta", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        when(proxyStore.get(any(String.class))).thenReturn(null);

        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual.keySet(), ImmutableSet.of("environment", "services"));
        assertEquals(actual.get("environment"), "testing");
        assertEqualsIgnoreOrder((Iterable<?>) actual.get("services"), ImmutableSet.of(
                toServiceRepresentation(redNodeId, red, redStorage),
                toServiceRepresentation(greenNodeId, green, greenStorage),
                toServiceRepresentation(blueNodeId, blue, blueStorage)
        ));

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/web")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual, ImmutableMap.of(
                "environment", "testing",
                "services", ImmutableList.of(
                        toServiceRepresentation(redNodeId, red, redWeb)
                )));

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/unknown")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual, ImmutableMap.of(
                "environment", "testing",
                "services", ImmutableList.of()));

        verify(proxyStore, times(3)).get(any(String.class));
        verifyNoMoreInteractions(proxyStore);
    }

    @Test
    public void testGetByTypeAndPool()
    {
        Id<Node> redNodeId = Id.random();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", of(redStorage, redWeb));

        Id<Node> greenNodeId = Id.random();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "alpha", "/x/y/z", of(greenStorage));

        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "beta", "/a/b/c", of(blueStorage));

        when(proxyStore.get(any(String.class), any(String.class))).thenReturn(null);

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/alpha")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual.keySet(), ImmutableSet.of("environment", "services"));
        assertEquals(actual.get("environment"), "testing");
        assertEqualsIgnoreOrder((Iterable<?>) actual.get("services"), ImmutableSet.of(
                toServiceRepresentation(redNodeId, red, redStorage),
                toServiceRepresentation(greenNodeId, green, greenStorage)
        ));

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/beta")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual, ImmutableMap.of(
                "environment", "testing",
                "services", ImmutableList.of(
                        toServiceRepresentation(blueNodeId, blue, blueStorage)
                )));

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/unknown")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual, ImmutableMap.of(
                "environment", "testing",
                "services", ImmutableList.of()));

        verify(proxyStore, times(3)).get(any(String.class), any(String.class));
        verifyNoMoreInteractions(proxyStore);
    }

    @Test
    public void testGetAll()
    {
        Id<Node> redNodeId = Id.random();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", of(redStorage, redWeb));

        Id<Node> greenNodeId = Id.random();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "alpha", "/x/y/z", of(greenStorage));

        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "beta", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        when(proxyStore.filterAndGetAll(any(Set.class))).thenAnswer(invocationOnMock -> invocationOnMock.getArguments()[0]);

        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual.keySet(), ImmutableSet.of("environment", "services"));
        assertEquals(actual.get("environment"), "testing");
        assertEqualsIgnoreOrder((Iterable<?>) actual.get("services"), ImmutableSet.of(
                toServiceRepresentation(redNodeId, red, redStorage),
                toServiceRepresentation(redNodeId, red, redWeb),
                toServiceRepresentation(greenNodeId, green, greenStorage),
                toServiceRepresentation(blueNodeId, blue, blueStorage)
        ));

        verify(proxyStore).filterAndGetAll(any(Set.class));
        verifyNoMoreInteractions(proxyStore);
    }

    @Test
    public void testProxyGetByType()
    {
        Id<Node> redNodeId = Id.random();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(Id.<Service>random() , "storage", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", of(redStorage, redWeb));

        Id<Node> greenNodeId = Id.random();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "alpha", "/x/y/z", of(greenStorage));

        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "beta", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        Service proxyStorageService = new Service(Id.<Service>random(), Id.<Node>random(), "storage", "general", "loc", ImmutableMap.of("key", "5"));
        when(proxyStore.get("storage")).thenReturn(of(proxyStorageService));

        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual, ImmutableMap.of(
                "environment", "testing",
                "services", ImmutableList.of(
                        toServiceRepresentation(proxyStorageService)
                )));

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/web")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual, ImmutableMap.of(
                "environment", "testing",
                "services", ImmutableList.of()));
    }

    @Test
    public void testProxyGetByTypeAndPool()
    {
        Id<Node> redNodeId = Id.random();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", of(redStorage, redWeb));

        Id<Node> greenNodeId = Id.random();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "alpha", "/x/y/z", of(greenStorage));

        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "beta", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        Service proxyStorageService = new Service(Id.<Service>random(), Id.<Node>random(), "storage", "alpha", "loc", ImmutableMap.of("key", "5"));
        when(proxyStore.get("storage", "alpha")).thenReturn(of(proxyStorageService));

        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/alpha")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual, ImmutableMap.of(
                "environment", "testing",
                "services", ImmutableList.of(
                        toServiceRepresentation(proxyStorageService)
                )));

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/beta")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual, ImmutableMap.of(
                "environment", "testing",
                "services", ImmutableList.of()));

        actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/unknown")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual, ImmutableMap.of(
                "environment", "testing",
                "services", ImmutableList.of()));
    }

    @Test
    public void testProxyGetAll()
    {
        Id<Node> redNodeId = Id.random();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", of(redStorage, redWeb));

        Id<Node> greenNodeId = Id.random();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "alpha", "/x/y/z", of(greenStorage));

        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "beta", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        final Service proxyStorageService = new Service(Id.<Service>random(), Id.<Node>random(), "storage", "alpha", "loc", ImmutableMap.of("key", "5"));
        when(proxyStore.filterAndGetAll(any(Set.class))).thenAnswer(invocationOnMock -> union(of(proxyStorageService),
                (Set<Service>) invocationOnMock.getArguments()[0]));

        Map<String, Object> actual = client.execute(
                prepareGet().setUri(uriFor("/v1/service")).build(),
                createJsonResponseHandler(mapCodec, OK.getStatusCode()));
        assertEquals(actual.keySet(), ImmutableSet.of("environment", "services"));
        assertEquals(actual.get("environment"), "testing");
        assertEqualsIgnoreOrder((Iterable<?>) actual.get("services"), ImmutableSet.of(
                toServiceRepresentation(proxyStorageService),
                toServiceRepresentation(redNodeId, red, redStorage),
                toServiceRepresentation(redNodeId, red, redWeb),
                toServiceRepresentation(greenNodeId, green, greenStorage),
                toServiceRepresentation(blueNodeId, blue, blueStorage)
        ));
    }

    @Test
    public void testGetByTypeInitializationPending()
    {
        when(initializationTracker.isPending()).thenReturn(true);

        StatusResponse response = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage")).build(),
                createStatusResponseHandler());
        assertEquals(response.getStatusCode(), 503);
    }

    @Test
    public void testGetByTypeAndPoolInitializationPending()
    {
        when(initializationTracker.isPending()).thenReturn(true);

        StatusResponse response = client.execute(
                prepareGet().setUri(uriFor("/v1/service/storage/alpha")).build(),
                createStatusResponseHandler());
        assertEquals(response.getStatusCode(), 503);
    }

    @Test
    public void testGetAllInitializationPending()
    {
        when(initializationTracker.isPending()).thenReturn(true);

        StatusResponse response = client.execute(
                prepareGet().setUri(uriFor("/v1/service")).build(),
                createStatusResponseHandler());
        assertEquals(response.getStatusCode(), 503);
    }

    private Map<String, Object> toServiceRepresentation(Id<Node> nodeId, DynamicAnnouncement dynamicAnnouncement, DynamicServiceAnnouncement dynamicServiceAnnouncement)
    {
        return ImmutableMap.<String, Object>builder()
                .put("id", dynamicServiceAnnouncement.getId().toString())
                .put("nodeId", nodeId.toString())
                .put("type", dynamicServiceAnnouncement.getType())
                .put("pool", dynamicAnnouncement.getPool())
                .put("location", dynamicAnnouncement.getLocation())
                .put("properties", dynamicServiceAnnouncement.getProperties())
                .build();
    }

    private Map<String, Object> toServiceRepresentation(Service service)
    {
        return ImmutableMap.<String, Object>builder()
                .put("id", service.getId().toString())
                .put("nodeId", service.getNodeId().toString())
                .put("type", service.getType())
                .put("pool", service.getPool())
                .put("location", service.getLocation())
                .put("properties", service.getProperties())
                .build();
    }

    private URI uriFor(String path)
    {
        return server.getBaseUrl().resolve(path);
    }
}
