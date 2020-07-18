using System;

namespace Akka.Deduplication.DData
{
    public class DeduplicationPruningConfiguration
    {
        public DeduplicationPruningConfiguration(TimeSpan pruningInterval,
            TimeSpan pruneCompletedAfter, TimeSpan pruneErroredAfter,
            TimeSpan? pruneNotAttemptedAfter, TimeSpan? pruneAttemptedAfter)
        {
            PruningInterval = pruningInterval;
            PruneNotAttemptedAfter = pruneNotAttemptedAfter;
            PruneAttemptedAfter = pruneAttemptedAfter;
            PruneCompletedAfter = pruneCompletedAfter;
            PruneErroredAfter = pruneErroredAfter;
        }
        public TimeSpan PruningInterval { get; }
        public TimeSpan? PruneNotAttemptedAfter { get; }
        public TimeSpan? PruneAttemptedAfter { get; }
        public TimeSpan PruneCompletedAfter { get; }
        public TimeSpan PruneErroredAfter { get; }
    }
}