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
import com.proofpoint.discovery.Id;
import com.proofpoint.discovery.Node;
import com.proofpoint.discovery.Service;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.jetty.JettyHttpClient;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.reporting.ReportExporter;
import com.proofpoint.testing.Closeables;
import com.proofpoint.testing.SerialScheduledExecutorService;
import com.proofpoint.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.proofpoint.discovery.store.Entry.entry;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

public class TestHttpRemoteStore
{
    private static final Id<Node> NODE_ID = Id.random();
    private static final Service TESTING_SERVICE_1 = new Service(Id.random(), NODE_ID,"type1", "test-pool", "/test-location", ImmutableMap.of("http", "http://127.0.0.1"));
    private static final Service TESTING_SERVICE_2 = new Service(Id.random(), NODE_ID,"type2", "test-pool", "/test-location", ImmutableMap.of("https", "https://127.0.0.1"));
    private static final JsonCodec<List<Service>> SERVICE_LIST_CODEC = JsonCodec.listJsonCodec(Service.class);
    private static final Entry TESTING_ENTRY = entry(
            NODE_ID.getBytes(),
            SERVICE_LIST_CODEC.toJsonBytes(ImmutableList.of(TESTING_SERVICE_1, TESTING_SERVICE_2)),
            System.currentTimeMillis(),
            20_000L
    );

    private final TestingStoreServer server = new TestingStoreServer(new StoreConfig());
    private final InMemoryStore serverStore = server.getInMemoryStore();
    private final HttpClient client = new JettyHttpClient();

    private SerialScheduledExecutorService executor;
    private HttpRemoteStore store;

    @BeforeMethod
    public void setup()
    {
        server.reset();
    }

    @AfterMethod(alwaysRun = true)
    public void shutdown()
    {
        if (store != null) {
            store.shutdown();
        }
        store = null;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        server.stop();
        Closeables.closeQuietly(client);
    }

    @Test
    public void testReplication()
            throws InterruptedException
    {
        createStore();
        store.put(TESTING_ENTRY);
        Thread.sleep(1000);

        assertEquals(serverStore.getAll(), ImmutableList.of(TESTING_ENTRY));
    }

    @Test
    public void testReplicationToAddedServer()
            throws InterruptedException
    {
        server.setServerInSelector(false);
        createStore();

        executor.elapseTimeNanosecondBefore(5, SECONDS);
        store.put(TESTING_ENTRY);
        Thread.sleep(1000);

        assertEquals(serverStore.getAll(), ImmutableList.of());

        server.setServerInSelector(true);
        executor.elapseTime(1, NANOSECONDS);
        store.put(TESTING_ENTRY);
        Thread.sleep(1000);

        assertEquals(serverStore.getAll(), ImmutableList.of(TESTING_ENTRY));
    }

    @Test
    public void testNoReplicationToRemovedServer()
            throws InterruptedException
    {
        createStore();

        executor.elapseTimeNanosecondBefore(5, SECONDS);
        server.setServerInSelector(false);
        executor.elapseTime(1, NANOSECONDS);
        store.put(TESTING_ENTRY);
        Thread.sleep(1000);

        assertEquals(serverStore.getAll(), ImmutableList.of());
    }

    private void createStore()
    {
        executor = new SerialScheduledExecutorService();
        store = new HttpRemoteStore("dynamic",
                new NodeInfo("test_environment"),
                server.getServiceSelector(),
                new StoreConfig().setRemoteUpdateInterval(new Duration(5, SECONDS)),
                client,
                mock(ReportExporter.class),
                executor);
        store.start();
    }
}
