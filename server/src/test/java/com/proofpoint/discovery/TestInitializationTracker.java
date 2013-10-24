/*
 * Copyright 2013 Proofpoint, Inc.
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

import com.proofpoint.discovery.InitializationTracker.CompletionNotifier;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestInitializationTracker
{
    @Test
    public void testNoTasks()
    {
        assertFalse(new InitializationTracker().isPending());
    }

    @Test
    public void testUncompletedTask()
    {
        InitializationTracker tracker = new InitializationTracker();
        tracker.createTask();
        assertTrue(tracker.isPending());
    }

    @Test
    public void testCompletedTasks()
    {
        InitializationTracker tracker = new InitializationTracker();
        CompletionNotifier task1 = tracker.createTask();
        CompletionNotifier task2 = tracker.createTask();
        task2.complete();
        task1.complete();
        assertFalse(tracker.isPending());
    }

    @Test
    public void testCompletionIdempotent()
    {
        InitializationTracker tracker = new InitializationTracker();
        CompletionNotifier task1 = tracker.createTask();
        CompletionNotifier task2 = tracker.createTask();
        task2.complete();
        task2.complete();
        assertTrue(tracker.isPending());
        task1.complete();
        assertFalse(tracker.isPending());
        task1.complete();
        assertFalse(tracker.isPending());
    }
}
