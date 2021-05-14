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
import com.proofpoint.discovery.InitializationTracker;
import com.proofpoint.discovery.Node;
import com.proofpoint.discovery.Service;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.balancing.HttpServiceBalancerStats;
import com.proofpoint.http.client.jetty.JettyHttpClient;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.reporting.testing.TestingReportCollectionFactory;
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
import static org.assertj.core.api.Assertions.assertThat;

public class TestReplicator
{
    private static final Id<Node> NODE_ID = Id.random();
    private static final Id<Node> TOMBSTONE_ID = Id.random();
    private static final Service TESTING_SERVICE_1 = new Service(Id.random(), NODE_ID,"type1", "test-pool", "/test-location", ImmutableMap.of("http", "http://127.0.0.1"));
    private static final Service TESTING_SERVICE_2 = new Service(Id.random(), NODE_ID,"type2", "test-pool", "/test-location", ImmutableMap.of("https", "https://127.0.0.1"));
    private static final Entry TESTING_ENTRY = entry(
            NODE_ID.getBytes(),
            ImmutableList.of(TESTING_SERVICE_1, TESTING_SERVICE_2),
            System.currentTimeMillis(),
            20_000L,
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
    private HttpServiceBalancerStats stats;
    private InMemoryStore inMemoryStore;
    private SerialScheduledExecutorService executor;
    private Replicator replicator;

    @BeforeMethod
    public void setup()
    {
        TestingReportCollectionFactory reportCollectionFactory = new TestingReportCollectionFactory();
        stats = reportCollectionFactory.createReportCollection(HttpServiceBalancerStats.class);
        inMemoryStore = new InMemoryStore();
        executor = new SerialScheduledExecutorService();
    }

    @AfterMethod(alwaysRun = true)
    public void shutdown() {
        if (replicator != null) {
            replicator.shutdown();
        }
        replicator = null;
        server.stop();
        server = null;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        Closeables.closeQuietly(client);
    }

    @Test
    public void testReplicationOnStartup()
    {
        replicator = createReplicator(true, TESTING_ENTRY, TESTING_TOMBSTONE);

        assertThat(inMemoryStore.getAll()).containsExactlyInAnyOrder(TESTING_ENTRY, TESTING_TOMBSTONE);
    }

    @Test
    public void testReplicationInterval()
    {
        replicator = createReplicator(true);

        executor.elapseTimeNanosecondBefore(1, SECONDS);
        assertThat(inMemoryStore.getAll()).isEmpty();

        serverStore.put(TESTING_ENTRY);
        serverStore.put(TESTING_TOMBSTONE);

        executor.elapseTime(1, NANOSECONDS);
        assertThat(inMemoryStore.getAll()).containsExactlyInAnyOrder(TESTING_ENTRY, TESTING_TOMBSTONE);
    }

    @Test
    public void testReplicationToAddedServer()
    {
        replicator = createReplicator(false);

        executor.elapseTimeNanosecondBefore(1, SECONDS);
        assertThat(inMemoryStore.getAll()).isEmpty();

        server.setServerInSelector(true);
        serverStore.put(TESTING_ENTRY);
        serverStore.put(TESTING_TOMBSTONE);

        executor.elapseTime(1, NANOSECONDS);
        assertThat(inMemoryStore.getAll()).containsExactlyInAnyOrder(TESTING_ENTRY, TESTING_TOMBSTONE);
    }

    @Test
    public void testNoReplicationToRemovedServer()
    {
        replicator = createReplicator(true);

        executor.elapseTimeNanosecondBefore(1, SECONDS);
        assertThat(inMemoryStore.getAll()).isEmpty();

        server.setServerInSelector(false);
        serverStore.put(TESTING_ENTRY);

        executor.elapseTime(1, NANOSECONDS);
        assertThat(inMemoryStore.getAll()).isEmpty();
    }

    private Replicator createReplicator(boolean serverInSelector, Entry... initialEntries)
    {
        server = new TestingStoreServer(new StoreConfig());
        serverStore = server.getInMemoryStore();
        server.setServerInSelector(serverInSelector);
        for (int i = 0; i < initialEntries.length; i++) {
            serverStore.put(initialEntries[i]);

        }
        Replicator replicator = new Replicator(
                "dynamic",
                new NodeInfo("test_environment"),
                server.getServiceSelector(),
                client,
                stats,
                inMemoryStore,
                new StoreConfig().setReplicationInterval(new Duration(1, SECONDS)),
                new InitializationTracker(),
                executor);
        replicator.start();
        return replicator;
    }
}
