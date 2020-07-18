using System;
using System.Linq;
using Akka.Actor;
using Akka.DistributedData;

namespace Akka.Deduplication.DData
{
	
	public abstract class DeduplicatingReceiverBase : ActorBase
	{
		protected readonly DistributedData.DistributedData Replicator =
			DistributedData.DistributedData.Get(Context.System);

		protected abstract string ReplicatorKey { get; }
		protected abstract int MaxAttempts { get; }

		protected void PurgeKey(string key)
		{
			var cluster = Cluster.Cluster.Get(Context.System);
			Replicator.Replicator.Tell(Dsl.Update(
				new LWWDictionaryKey<string, DeduplicationState>(ReplicatorKey),
				WriteLocal.Instance,
				dict =>
				{
					if (dict != null)
					{
						return dict.Remove(cluster.SelfUniqueAddress, key);
					}

					return LWWDictionary<string, DeduplicationState>.Empty;
				}));
		}

		protected DeduplicationState PeekCurrentState(string key)
		{
			return Replicator
				.GetAsync(
					new LWWDictionaryKey<string, DeduplicationState>(
						ReplicatorKey)).Result.FirstOrDefault(r => r.Key == key)
				.Value;
		}

		protected DeduplicationState AdvanceAndGetProcessingState(string key)
		{
			Replicator.Replicator.Tell(CreateAdvanceAndGetStateCommand(key));
			return Replicator
				.GetAsync(
					new LWWDictionaryKey<string, DeduplicationState>(
						ReplicatorKey), Dsl.ReadLocal).Result
				.FirstOrDefault(r => r.Key == key).Value;
		}

		protected void MarkCompletion(string key)
		{
			Replicator.Replicator.Tell(CreateUpdateStateCompletedCommand(key));
		}

		protected Update CreateUpdateStateCompletedCommand(string innerKey)
		{
			var cluster = Cluster.Cluster.Get(Context.System);
			return Dsl.Update(
				key: new LWWDictionaryKey<string, DeduplicationState>(
					ReplicatorKey),
				initial: LWWDictionary.Create(node: cluster.SelfUniqueAddress,
					key: innerKey,
					value: new DeduplicationState()
					{
						NumberAttempts = 0,
						ProcessingState = DeduplicationProcessingState.Processed
					})
				, consistency: new WriteMajority(TimeSpan.FromSeconds(5)),
				modify: dds =>
				{
					if (dds.TryGetValue(innerKey, out DeduplicationState state))
					{
						state.ProcessingState =
							DeduplicationProcessingState.Processed;
						return dds.SetItem(cluster, innerKey, state);
					}
					else
					{
						return dds.SetItem(cluster, innerKey,
							new DeduplicationState()
							{
								NumberAttempts = 0,
								ProcessingState = DeduplicationProcessingState
									.Processed
							});
					}

				});
		}

		protected Update CreateAdvanceAndGetStateCommand(string innerKey)
		{
			var cluster = Cluster.Cluster.Get(Context.System);
			
			return Dsl.Update(
				key: new LWWDictionaryKey<string, DeduplicationState>(
					ReplicatorKey),
				initial: LWWDictionary.Create(node: cluster.SelfUniqueAddress,
					key: innerKey,
					value: new DeduplicationState()
					{
						NumberAttempts = 0,
						ProcessingState =
							DeduplicationProcessingState.NotAttempted
					}
				)
				, consistency: new WriteMajority(TimeSpan.FromSeconds(5)),
				modify: dds =>
				{
					if (dds.TryGetValue(innerKey, out DeduplicationState state))
					{

						if (state.ProcessingState ==
						    DeduplicationProcessingState.Error ||
						    state.ProcessingState ==
						    DeduplicationProcessingState.Processed)
						{
							return dds;
						}
						else if (state.NumberAttempts < MaxAttempts)
						{
							state.NumberAttempts = state.NumberAttempts + 1;
							state.ProcessingState = DeduplicationProcessingState
								.Attempted;
							return dds.SetItem(cluster, innerKey, state);
						}
						else
						{
							state.ProcessingState =
								DeduplicationProcessingState.Error;
							return dds.SetItem(cluster, innerKey, state);
						}
					}
					else
					{
						return dds.SetItem(cluster, innerKey,
							new DeduplicationState()
							{
								NumberAttempts = 0,
								ProcessingState = DeduplicationProcessingState
									.NotAttempted
							});
					}
				});
		}
	}
}