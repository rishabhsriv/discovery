/*
 * Copyright 2015 Proofpoint, Inc.
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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.discovery.Service;
import com.proofpoint.json.JsonCodec;
import org.joda.time.DateTime;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Sets.intersection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DynamicUpdateListener
    implements UpdateListener
{
    private static final JsonCodec<List<Service>> CODEC = JsonCodec.listJsonCodec(Service.class);

    private final Supplier<DateTime> timeSupplier;
    private final DynamicRenewals dynamicRenewals;

    @Inject
    public DynamicUpdateListener(Supplier<DateTime> timeSupplier, DynamicRenewals dynamicRenewals)
    {
        this.timeSupplier = checkNotNull(timeSupplier, "timeSupplier is null");
        this.dynamicRenewals = checkNotNull(dynamicRenewals, "dynamicRenewals is null");
    }

    @Override
    public void notifyUpdate(Entry oldEntry, Entry newEntry)
    {
        if (newEntry.getValue() != null && oldEntry.getValue() != null) {
            long renewedAfterMillis = timeSupplier.get().getMillis() - oldEntry.getTimestamp();
            if (renewedAfterMillis > oldEntry.getMaxAgeInMs()) {
                for (String type : intersection(getTypes(oldEntry), getTypes(newEntry))) {
                    dynamicRenewals.expiredFor(type).add(renewedAfterMillis - oldEntry.getMaxAgeInMs(), MILLISECONDS);
                }
            }
            else {
                for (String type : intersection(getTypes(oldEntry), getTypes(newEntry))) {
                    dynamicRenewals.renewedAfter(type).add(renewedAfterMillis, MILLISECONDS);
                }
            }
        }
    }

    private static Set<String> getTypes(Entry newEntry)
    {
        return ImmutableSet.copyOf(transform(CODEC.fromJson(newEntry.getValue()), Service::getType));
    }
}
