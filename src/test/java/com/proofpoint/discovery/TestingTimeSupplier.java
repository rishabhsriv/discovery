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

import com.proofpoint.units.Duration;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

class TestingTimeSupplier
        implements Supplier<Instant>
{
    private final AtomicLong currentTime = new AtomicLong(System.currentTimeMillis());

    public void add(Duration interval)
    {
        currentTime.addAndGet(interval.toMillis());
    }

    public void set(Instant currentTime)
    {
        this.currentTime.set(currentTime.toEpochMilli());
    }

    public void increment()
    {
        currentTime.incrementAndGet();
    }

    @Override
    public Instant get()
    {
        return Instant.ofEpochMilli(currentTime.get());
    }
}
