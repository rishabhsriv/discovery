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
import com.proofpoint.discovery.DiscoveryConfig;
import com.proofpoint.discovery.Id;
import com.proofpoint.discovery.Node;
import com.proofpoint.discovery.Service;
import com.proofpoint.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.proofpoint.discovery.store.Entry.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestInMemoryStore
{
    private static final Id<Node> NODE_ID = Id.random();
    private static final Service TESTING_SERVICE_1 = new Service(Id.random(), NODE_ID,"type1", "test-pool", "/test-location", ImmutableMap.of("http", "http://127.0.0.1"));
    private static final Service TESTING_SERVICE_2 = new Service(Id.random(), NODE_ID,"type2", "test-pool", "/test-location", ImmutableMap.of("https", "https://127.0.0.1"));
    private static final List<Service> SERVICE_LIST_1 = List.of(TESTING_SERVICE_1, TESTING_SERVICE_2);
    private static final List<Service> SERVICE_LIST_2 = List.of(TESTING_SERVICE_2);

    private InMemoryStore store;
    private UpdateListener updateListener;

    @BeforeMethod
    protected void setUp()
    {
        DiscoveryConfig config = new DiscoveryConfig().setMaxAge(new Duration(1, TimeUnit.MINUTES));
        store = new InMemoryStore(config);
        updateListener = mock(UpdateListener.class);
        store.setUpdateListener(updateListener);
    }

    @Test
    public void testPut()
    {
        Entry entry = entryOf(SERVICE_LIST_1, 1);
        assertTrue(store.put(entry));

        assertThat(store.get(NODE_ID.getBytes())).isEqualTo(entry);
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testPutAlreadyThere()
    {
        Entry entry = entryOf(SERVICE_LIST_1, 1);
        assertTrue(store.put(entry));
        assertFalse(store.put(entryOf(SERVICE_LIST_1, 1)));

        assertThat(store.get(NODE_ID.getBytes())).isEqualTo(entry);
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testDelete()
    {
        byte[] key = NODE_ID.getBytes();
        Entry entry = entryOf(SERVICE_LIST_1, 1);
        store.put(entry);

        assertTrue(store.delete(key, entry.getTimestamp()));

        assertThat(store.get(key)).isNull();
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testDeleteMissingEntry()
    {
        byte[] key = NODE_ID.getBytes();

        assertFalse(store.delete(key, 1));

        assertThat(store.get(key)).isNull();
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testDeleteOlderVersion()
    {
        byte[] key = NODE_ID.getBytes();
        Entry entry = entryOf(SERVICE_LIST_1, 5);
        store.put(entry);

        assertFalse(store.delete(key, 2));

        assertThat(store.get(NODE_ID.getBytes())).isEqualTo(entry);
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testUpdate()
    {
        Entry entry1 = entryOf(SERVICE_LIST_2, 1);
        assertTrue(store.put(entry1));

        Entry entry2 = entryOf(SERVICE_LIST_1, 2);
        assertTrue(store.put(entry2));

        assertThat(store.get(NODE_ID.getBytes())).isEqualTo(entry2);
        verify(updateListener).notifyUpdate(entry1, entry2);
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testUpdateNoListener()
    {
        store = new InMemoryStore(new DiscoveryConfig().setMaxAge(new Duration(1, TimeUnit.MINUTES)));
        Entry entry1 = entryOf(SERVICE_LIST_2, 1);
        assertTrue(store.put(entry1));

        Entry entry2 = entryOf(SERVICE_LIST_1, 2);
        assertTrue(store.put(entry2));

        assertThat(store.get(NODE_ID.getBytes())).isEqualTo(entry2);
    }

    @Test
    public void testResolvesConflict()
    {
        Entry entry2 = entryOf(SERVICE_LIST_1, 2);
        assertTrue(store.put(entry2));

        Entry entry1 = entryOf(SERVICE_LIST_2, 1);
        assertFalse(store.put(entry1));

        assertThat(store.get(NODE_ID.getBytes())).isEqualTo(entry2);
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testDefaultsMaxAge()
    {
        Entry entry = entryOf(SERVICE_LIST_1, 1);
        store.put(entry(entry.getKey(), entry.getValue(), entry.getTimestamp(), null, "127.0.0.1"));

        assertThat(store.get(NODE_ID.getBytes())).isEqualTo(entry);
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testDoesntDefaultMaxAge()
    {
        store = new InMemoryStore();

        Entry entry = entryOf(SERVICE_LIST_1, 1);
        entry = entry(entry.getKey(), entry.getValue(), entry.getTimestamp(), null, "127.0.0.1");
        store.put(entry);

        assertThat(store.get(NODE_ID.getBytes())).isEqualTo(entry);
        verifyNoMoreInteractions(updateListener);
    }

    private static Entry entryOf(List<Service> value, long timestamp)
    {
        return entry(NODE_ID.getBytes(), value, timestamp, 60_000L, "127.0.0.1");
    }
}
