using Akka.Cluster;

namespace Akka.Deduplication.DData
{
    public interface IDeduplicatingReceiver
    {
        DeduplicationPruningConfiguration PruningConfiguration { get; }
        DistributedData.DistributedData Replicator { get; }
        UniqueAddress SelfUniqueAddress { get; }
        string ReplicatorKey { get; }
        int MaxAttempts { get; }
    }
}