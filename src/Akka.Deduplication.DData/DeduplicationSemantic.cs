using System;
using System.Linq;
using Akka.Actor;
using Akka.DistributedData;

namespace Akka.Deduplication.DData
{
    public class DeduplicationSemantic
    {
        private IDeduplicatingReceiver _receiver;

        public DeduplicationSemantic(IDeduplicatingReceiver receiver)
        {
            this._receiver = receiver;
        }
        public void PruneEntries(IGetResponse g)
        {
            var entries = g
                .Get(new LWWDictionaryKey<string, DeduplicationState>(
                    this._receiver.ReplicatorKey));

            var toPrune = entries.Where(r =>
                r.Value?.PruneAfter != null && r.Value.PruneAfter <= DateTime.Now);
            foreach (var deduplicationState in toPrune)
            {
                PurgeKey(deduplicationState.Key);
            }
        }
        public void PurgeKey(string key, IWriteConsistency writeConsistency = null)
        {
            var useWriteConsistency = writeConsistency ?? WriteLocal.Instance;
            _receiver.Replicator.Replicator.Tell(Dsl.Update(
                new LWWDictionaryKey<string, DeduplicationState>(_receiver
                    .ReplicatorKey),
                WriteLocal.Instance,
                dict =>
                {
                    if (dict != null)
                    {
                        return dict.Remove(_receiver.SelfUniqueAddress, key);
                    }

                    return LWWDictionary<string, DeduplicationState>.Empty;
                }));
        }

        /// <summary>
        /// Peeks at the current state without advancing
        /// </summary>
        /// <param name="key">The Key Entry to retrieve state for</param>
        /// <param name="readConsistency">The Consistency to use for reading. if null, <see cref="ReadLocal"/> will be used</param>
        /// <returns>The current Deduplication state</returns>
        public DeduplicationState PeekCurrentState(string key, IReadConsistency readConsistency= null)
        {
            return _receiver.Replicator
                .GetAsync(
                    new LWWDictionaryKey<string, DeduplicationState>(
                        _receiver.ReplicatorKey), readConsistency).Result.FirstOrDefault(r => r.Key == key)
                .Value;
        }

        public DeduplicationState AdvanceAndGetProcessingState(string key, IWriteConsistency writeConsistency = null, IReadConsistency readConsistency=null)
        {
            var _writeconsistency = writeConsistency ?? WriteLocal.Instance;
            _receiver.Replicator.Replicator.Tell(CreateAdvanceAndGetStateCommand(key,_writeconsistency));
            return _receiver.Replicator
                .GetAsync(
                    new LWWDictionaryKey<string, DeduplicationState>(
                        _receiver.ReplicatorKey),readConsistency).Result
                .FirstOrDefault(r => r.Key == key).Value;
        }

        public void MarkCompletion(string key, IWriteConsistency writeConsistency = null)
        {
            var _writeconsistency = writeConsistency ?? WriteLocal.Instance;
            _receiver.Replicator.Replicator.Tell(CreateUpdateStateCompletedCommand(key,_writeconsistency));
        }

        protected Update CreateUpdateStateCompletedCommand(string innerKey, IWriteConsistency writeConsistency=null)
        {

            return Dsl.Update(
                key: new LWWDictionaryKey<string, DeduplicationState>(
                    _receiver.ReplicatorKey),
                initial: LWWDictionary.Create(node: _receiver.SelfUniqueAddress,
                    key: innerKey,
                    value: new DeduplicationState()
                    {
                        NumberAttempts = 0,
                        ProcessingState =
                            DeduplicationProcessingState.Processed,
                        PruneAfter =
                            _receiver.PruningConfiguration
                                .PruneAttemptedAfter != null
                                ? DateTime.UtcNow + _receiver
                                    .PruningConfiguration.PruneAttemptedAfter
                                : null
                    })
                , consistency: writeConsistency,
                modify: dds =>
                {
                    if (dds.TryGetValue(innerKey, out DeduplicationState state))
                    {
                        state.ProcessingState =
                            DeduplicationProcessingState.Processed;
                        state.PruneAfter =
                            DateTime.UtcNow + _receiver
                                .PruningConfiguration.PruneCompletedAfter;

                        return dds.SetItem(_receiver.SelfUniqueAddress,
                            innerKey, state);
                    }
                    else
                    {
                        return dds.SetItem(_receiver.SelfUniqueAddress,
                            innerKey,
                            new DeduplicationState()
                            {
                                NumberAttempts = 0,
                                ProcessingState = DeduplicationProcessingState
                                    .Processed,
                                PruneAfter = DateTime.UtcNow + _receiver
                                    .PruningConfiguration
                                    .PruneCompletedAfter
                            });
                    }

                });
        }

        protected Update CreateAdvanceAndGetStateCommand(string innerKey, IWriteConsistency writeConsistency)
        {
			
            return Dsl.Update(
                key: new LWWDictionaryKey<string, DeduplicationState>(
                    _receiver.ReplicatorKey),
                initial: LWWDictionary.Create(node: _receiver.SelfUniqueAddress,
                    key: innerKey,
                    value: new DeduplicationState()
                    {
                        NumberAttempts = 0,
                        ProcessingState =
                            DeduplicationProcessingState.NotAttempted,
                        PruneAfter = _receiver.PruningConfiguration
                            .PruneNotAttemptedAfter != null
                            ? DateTime.UtcNow + _receiver
                                .PruningConfiguration.PruneNotAttemptedAfter
                            : (DateTime?)null
                    }
                )
                , consistency: writeConsistency,
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
                        else if (state.NumberAttempts < _receiver.MaxAttempts)
                        {
                            state.NumberAttempts = state.NumberAttempts + 1;
                            state.ProcessingState = DeduplicationProcessingState
                                .Attempted;
                            state.PruneAfter = _receiver.PruningConfiguration
                                .PruneAttemptedAfter != null
                                ? DateTime.UtcNow + _receiver
                                    .PruningConfiguration.PruneAttemptedAfter
                                : (DateTime?)null;
                            return dds.SetItem(_receiver.SelfUniqueAddress, innerKey, state);
                        }
                        else
                        {
                            state.ProcessingState =
                                DeduplicationProcessingState.Error;
                            state.PruneAfter =
                                DateTime.UtcNow + _receiver
                                    .PruningConfiguration.PruneErroredAfter;
                            return dds.SetItem(_receiver.SelfUniqueAddress,
                                innerKey, state);
                        }
                    }
                    else
                    {
                        return dds.SetItem(_receiver.SelfUniqueAddress, innerKey,
                            new DeduplicationState()
                            {
                                NumberAttempts = 0,
                                ProcessingState = DeduplicationProcessingState
                                    .NotAttempted,
                                PruneAfter = _receiver.PruningConfiguration
                                    .PruneNotAttemptedAfter != null
                                    ? DateTime.UtcNow + _receiver
                                        .PruningConfiguration.PruneNotAttemptedAfter
                                    : (DateTime?)null
                            });
                    }
                });
        }
    }
}