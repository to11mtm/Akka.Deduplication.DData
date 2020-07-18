using System;
using System.Linq;
using Akka.Actor;
using Akka.Cluster;
using Akka.DistributedData;

namespace Akka.Deduplication.DData
{
	public class PurgeStateQuery
	{
		
	}
	public abstract class DeduplicatingActorBase : ActorBase, IDeduplicatingReceiver
	{
		public abstract  DeduplicationPruningConfiguration PruningConfiguration { get; }

		public DistributedData.DistributedData Replicator =>
			DistributedData.DistributedData.Get(Context.System);
		 public UniqueAddress SelfUniqueAddress => Cluster.Cluster.Get(Context.System).SelfUniqueAddress;
		 public abstract string ReplicatorKey { get; }
		public  abstract int MaxAttempts { get; }

		public DeduplicatingActorBase()
		{
			Deduplication = new DeduplicationSemantic(this);
		}

		protected sealed override bool Receive(object message)
		{
			if (message is IGetResponse g && g.Request is PurgeStateQuery)
			{
				Deduplication.PruneEntries(g);

				return true;
			}
			else
			{
				return ReceiveCommand(message);
			}
		}

		

		protected abstract bool ReceiveCommand(object message);
		

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

		private ICancelable _pruningToken;

		protected readonly DeduplicationSemantic Deduplication;
	}
}