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

import com.google.common.base.Supplier;
import com.proofpoint.discovery.store.DistributedStore;
import com.proofpoint.discovery.store.InMemoryStore;
import com.proofpoint.discovery.store.RemoteStore;
import com.proofpoint.discovery.store.StoreConfig;
import com.proofpoint.units.Duration;
import org.joda.time.DateTime;

import java.util.concurrent.TimeUnit;

public class TestReplicatedStaticStore
    extends TestStaticStore
{
    @Override
    protected StaticStore initializeStore(Supplier<DateTime> timeSupplier)
    {
        RemoteStore dummy = entry -> { };
        DiscoveryConfig config = new DiscoveryConfig().setMaxAge(new Duration(1, TimeUnit.MINUTES));
        DistributedStore distributedStore = new DistributedStore("static", new InMemoryStore(config), dummy, new StoreConfig(), timeSupplier);

        return new ReplicatedStaticStore(distributedStore);
    }
}
