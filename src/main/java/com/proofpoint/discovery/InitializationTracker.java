package com.proofpoint.discovery;

import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.Set;

public class InitializationTracker
{
    private final Set<CompletionNotifier> notifiers = new ConcurrentHashSet<>();

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
