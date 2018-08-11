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

import com.proofpoint.discovery.DiscoveryConfig;
import com.proofpoint.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.proofpoint.discovery.store.Entry.entry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestInMemoryStore
{
    private InMemoryStore store;
    private UpdateListener updateListener;

    @BeforeMethod
    protected void setUp()
            throws Exception
    {
        DiscoveryConfig config = new DiscoveryConfig().setMaxAge(new Duration(1, TimeUnit.MINUTES));
        store = new InMemoryStore(config);
        updateListener = mock(UpdateListener.class);
        store.setUpdateListener(updateListener);
    }

    @Test
    public void testPut()
    {
        Entry entry = entryOf("blue", "apple", 1);
        assertTrue(store.put(entry));

        assertEquals(store.get("blue".getBytes(UTF_8)), entry);
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testPutAlreadyThere()
    {
        Entry entry = entryOf("blue", "apple", 1);
        assertTrue(store.put(entry));
        assertFalse(store.put(entryOf("blue", "apple", 1)));

        assertEquals(store.get("blue".getBytes(UTF_8)), entry);
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testDelete()
    {
        byte[] key = "blue".getBytes(UTF_8);
        Entry entry = entryOf("blue", "apple", 1);
        store.put(entry);

        assertTrue(store.delete(key, entry.getTimestamp()));

        assertNull(store.get(key));
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testDeleteMissingEntry()
    {
        byte[] key = "blue".getBytes(UTF_8);

        assertFalse(store.delete(key, 1));

        assertNull(store.get(key));
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testDeleteOlderVersion()
    {
        byte[] key = "blue".getBytes(UTF_8);
        Entry entry = entryOf("blue", "apple", 5);
        store.put(entry);

        assertFalse(store.delete(key, 2));

        assertEquals(store.get("blue".getBytes(UTF_8)), entry);
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testUpdate()
    {
        Entry entry1 = entryOf("blue", "banana", 1);
        assertTrue(store.put(entry1));

        Entry entry2 = entryOf("blue", "apple", 2);
        assertTrue(store.put(entry2));

        assertEquals(store.get("blue".getBytes(UTF_8)), entry2);
        verify(updateListener).notifyUpdate(entry1, entry2);
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testUpdateNoListener()
    {
        store = new InMemoryStore(new DiscoveryConfig().setMaxAge(new Duration(1, TimeUnit.MINUTES)));
        Entry entry1 = entryOf("blue", "banana", 1);
        assertTrue(store.put(entry1));

        Entry entry2 = entryOf("blue", "apple", 2);
        assertTrue(store.put(entry2));

        assertEquals(store.get("blue".getBytes(UTF_8)), entry2);
    }

    @Test
    public void testResolvesConflict()
    {
        Entry entry2 = entryOf("blue", "apple", 2);
        assertTrue(store.put(entry2));

        Entry entry1 = entryOf("blue", "banana", 1);
        assertFalse(store.put(entry1));

        assertEquals(store.get("blue".getBytes(UTF_8)), entry2);
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testDefaultsMaxAge()
    {
        Entry entry = entryOf("blue", "apple", 1);
        store.put(entry(entry.getKey(), entry.getValue(), entry.getTimestamp(), null));

        assertEquals(store.get("blue".getBytes(UTF_8)), entry);
        verifyNoMoreInteractions(updateListener);
    }

    @Test
    public void testDoesntDefaultMaxAge()
    {
        store = new InMemoryStore();

        Entry entry = entryOf("blue", "apple", 1);
        entry = entry(entry.getKey(), entry.getValue(), entry.getTimestamp(), null);
        store.put(entry);

        assertEquals(store.get("blue".getBytes(UTF_8)), entry);
        verifyNoMoreInteractions(updateListener);
    }

    private static Entry entryOf(String key, String value, long timestamp)
    {
        return entry(key.getBytes(UTF_8), value.getBytes(UTF_8), timestamp, 60_000L);
    }
}
