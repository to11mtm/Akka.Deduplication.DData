using Akka.Actor;
using Akka.Cluster;
using Akka.DistributedData;

namespace Akka.Deduplication.DData
{
    public abstract class DeduplicatingReceiveActorBase : ReceiveActor,
        IDeduplicatingReceiver
    {
        public abstract DeduplicationPruningConfiguration PruningConfiguration { get; }

        public DistributedData.DistributedData Replicator =>
            DistributedData.DistributedData.Get(Context.System);
        public UniqueAddress SelfUniqueAddress => Cluster.Cluster.Get(Context.System).SelfUniqueAddress;
        public abstract string ReplicatorKey { get; }
        public  abstract int MaxAttempts { get; }

        public DeduplicatingReceiveActorBase()
        {
            Deduplication = new DeduplicationSemantic(this);
            Receive<IGetResponse>(Deduplication.PruneEntries,
                gr => gr.Request is PurgeStateQuery);
        }
        protected override void PreStart()
        {
            _pruningToken =
                Context.System.Scheduler.ScheduleTellRepeatedlyCancelable(
                    PruningConfiguration.PruningInterval,
                    PruningConfiguration.PruningInterval, Replicator.Replicator,
                    Dsl.Get(
                        new LWWDictionaryKey<string, DeduplicationState>(
                            ReplicatorKey),request:new PurgeStateQuery()), Self);
        }

        protected override void PostStop()
        {
            _pruningToken?.Cancel();
        }
        protected readonly DeduplicationSemantic Deduplication;
        private ICancelable _pruningToken;
    }
}