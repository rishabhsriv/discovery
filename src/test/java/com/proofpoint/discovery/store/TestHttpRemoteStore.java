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
import com.proofpoint.node.NodeInfo;
import com.proofpoint.reporting.ReportExporter;
import com.proofpoint.testing.Closeables;
import com.proofpoint.testing.SerialScheduledExecutorService;
import com.proofpoint.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.proofpoint.discovery.store.Entry.entry;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class TestHttpRemoteStore
{
    private static final Id<Node> NODE_ID = Id.random();
    private static final Id<Node> TOMBSTONE_ID = Id.random();
    private static final Service TESTING_SERVICE_1 = new Service(Id.random(), NODE_ID, "type1", "test-pool", "/test-location", ImmutableMap.of("http", "http://127.0.0.1"));
    private static final Service TESTING_SERVICE_2 = new Service(Id.random(), NODE_ID, "type2", "test-pool", "/test-location", ImmutableMap.of("https", "https://127.0.0.1"));
    private static final Entry TESTING_ENTRY = entry(
            NODE_ID.getBytes(),
            ImmutableList.of(TESTING_SERVICE_1, TESTING_SERVICE_2),
            System.currentTimeMillis(),
            2_000_000L,
            "127.0.0.1"
    );
    private static final Entry TESTING_TOMBSTONE = entry(
            TOMBSTONE_ID.getBytes(),
            (List<Service>) null,
            System.currentTimeMillis(),
            null,
            null
    );

    private final HttpClient client = new JettyHttpClient();

    private TestingStoreServer server;
    private InMemoryStore serverStore;
    private SerialScheduledExecutorService executor;
    private HttpRemoteStore store;

    @AfterMethod(alwaysRun = true)
    public void shutdown()
    {
        if (store != null) {
            store.shutdown();
        }
        store = null;
        server.stop();
        server = null;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        Closeables.closeQuietly(client);
    }

    @Test
    public void testReplication()
            throws InterruptedException
    {
        createStore(true);
        store.put(TESTING_ENTRY);
        store.put(TESTING_TOMBSTONE);
        Thread.sleep(1000);

        assertThat(serverStore.getAll()).containsExactlyInAnyOrder(TESTING_ENTRY, TESTING_TOMBSTONE);
    }

    @Test
    public void testReplicationToAddedServer()
            throws InterruptedException
    {
        createStore(false);

        executor.elapseTimeNanosecondBefore(5, SECONDS);
        store.put(TESTING_ENTRY);
        Thread.sleep(1000);

        assertThat(serverStore.getAll()).isEmpty();

        server.setServerInSelector(true);
        executor.elapseTime(1, NANOSECONDS);
        store.put(TESTING_ENTRY);
        store.put(TESTING_TOMBSTONE);
        Thread.sleep(1000);

        assertThat(serverStore.getAll()).containsExactlyInAnyOrder(TESTING_ENTRY, TESTING_TOMBSTONE);
    }

    @Test
    public void testNoReplicationToRemovedServer()
            throws InterruptedException
    {
        createStore(true);

        executor.elapseTimeNanosecondBefore(5, SECONDS);
        server.setServerInSelector(false);
        executor.elapseTime(1, NANOSECONDS);
        store.put(TESTING_ENTRY);
        Thread.sleep(1000);

        assertThat(serverStore.getAll()).isEmpty();
    }

    private void createStore(boolean serverInSelector)
    {
        server = new TestingStoreServer(new StoreConfig());
        serverStore = server.getInMemoryStore();
        server.setServerInSelector(serverInSelector);
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
