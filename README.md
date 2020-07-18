# Akka.Deduplication.DData

The goal of this library is to provide a performant and efficient way to de-duplicate messages.  

## Work in progress

This is currently still a work in progress and should be used in production at your own risk!

 Notable concerns:
 
  - Has not been tested under heavy load scenarios to determine memory usage.
  
  - Currently does not have unit tests around automatic pruning. 

## How to use:

When utilizing At-Least-Once Delivery Actors, you should have a unique identifier for your message that is persisted as part of the event/command.
This unique identifier should be sent with the rest of your message to your implementation of `DeduplicatingActorBase` or `DeduplicatingReceiveActorBase`.

Inside this receiver, you may use the `Deduplicator` property to call the following methods. If no consistency is provided, `(Read/Write)Local` will be used.

```c#
Deduplicator.AdvanceAndGetProcessingState(string key,
          IWriteConsistency writeConsistency = null, 
          IReadConsistency readConsistency=null)
```

The above will advance the state and return it, based upon the following rules:
 - Unseen entries will have a state of 'NotAttempted' and Attempt Count of 0
 - Entries with an attempt count of less than `MaxRetries` and not `Processed` or `Error` will have their attempt count incremented and state set to 'attempted'.
 - Entries with an attempt count equal to `MaxRetries` and not in a `Processed` or `Error` state will have their state set to `Error`
 
 ```c#
MarkCompletion(string key, IWriteConsistency writeConsistency = null)
``` 
This will mark the entry as `Processed`

```c#
PeekCurrentState(string key, IReadConsistency readConsistency=null)
```
This will peek at the current state without advancing it. May be useful for variations on deduplication behavior or otherwise checking state.

```c#
PurgeKey(string key, IWriteConsistency writeConsistency = null)
```

This will remove the key in question from the DData storage. This method should only be called when you have a certainty that all pending deliveries have been received by the recipient before this is called.

