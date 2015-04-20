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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Throwables;
import com.proofpoint.log.Logger;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class PersistentStore
    implements LocalStore
{
    private static final Logger log = Logger.get(PersistentStore.class);
    private final DB db;
    private final ObjectMapper mapper = new ObjectMapper(new SmileFactory()).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    private final InMemoryStore cache;

    @Inject
    public PersistentStore(PersistentStoreConfig config)
            throws IOException
    {
        db = Iq80DBFactory.factory.open(config.getLocation(), new Options().createIfMissing(true));
        cache = new InMemoryStore();

        for (Map.Entry<byte[], byte[]> dbEntry : db) {
            try {
                cache.put(mapper.readValue(dbEntry.getValue(), Entry.class));
            }
            catch (IOException e) {
                byte[] key = dbEntry.getKey();
                log.error(e, "Corrupt entry " + Arrays.toString(key));

                // delete the corrupt entry... if another node has a non-corrupt version it will be replicated
                db.delete(key);
            }
        }
    }

    @Override
    public synchronized boolean put(Entry entry)
    {
        if (!cache.put(entry)) {
            return false;
        }

        byte[] dbEntry;
        try {
            dbEntry = mapper.writeValueAsBytes(entry);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        db.put(entry.getKey(), dbEntry);
        return true;
    }

    @Override
    public Entry get(byte[] key)
    {
        return cache.get(key);
    }

    @Override
    public synchronized boolean delete(byte[] key, long timestamp)
    {
        if (!cache.delete(key, timestamp)) {
            return false;
        }
        db.delete(key);
        return true;
    }

    @Override
    public Iterable<Entry> getAll()
    {
        return cache.getAll();
    }
}
