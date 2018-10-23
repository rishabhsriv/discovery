package com.proofpoint.discovery;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InitializationTracker
{
    private final Set<CompletionNotifier> notifiers = ConcurrentHashMap.newKeySet();

    public boolean isPending()
    {
        return !notifiers.isEmpty();
    }

    public CompletionNotifier createTask()
    {
        CompletionNotifier notifier = new CompletionNotifier();
        notifiers.add(notifier);
        return notifier;
    }

    public class CompletionNotifier
    {
        public void complete()
        {
            notifiers.remove(this);
        }
    }
}
