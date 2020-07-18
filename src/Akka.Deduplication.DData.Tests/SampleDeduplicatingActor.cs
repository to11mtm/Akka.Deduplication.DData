using System;
using Akka.Actor;
using Akka.DistributedData;
using Akka.Event;

namespace Akka.Deduplication.DData.Tests
{
    
    public class SampleDeduplicatingActor : DeduplicatingActorBase
    {
        public static int FirstTimeCalledCount = 0;
        public static int DelayCallCount = 0;
        public SampleDeduplicatingActor(string replicatorKey, int maxAttempts)
        {
            PruningConfiguration = new DeduplicationPruningConfiguration(
                TimeSpan.FromMinutes(5), TimeSpan.FromDays(5),
                TimeSpan.FromDays(5), null, null);
            ReplicatorKey = replicatorKey;
            MaxAttempts = maxAttempts;
        }

        protected override void PreRestart(Exception reason, object message)
        {
            Context.GetLogger().Error(reason,"oops");
            base.PreRestart(reason, message);
        }

        protected override bool ReceiveCommand(object message)
        {
            if (message is PurgeEntry purge)
            {
                Deduplication.PurgeKey(purge.Key);
            }
            if (message is PingPong)
            {
                Context.Sender.Tell(message);
            }
            if (message is IAtMostOnceMessage amom)
            {
                var state = Deduplication.AdvanceAndGetProcessingState(amom.TestKey);
                if (state.ProcessingState ==
                    DeduplicationProcessingState.Attempted ||
                    state.ProcessingState ==
                    DeduplicationProcessingState.NotAttempted)
                {
                    if (message is WorkFirstTimeWithDelay delay)
                    {
                        DelayCallCount = DelayCallCount + 1;
                    }
                    if (amom is FailBoat)
                    {
                        FailBoatCalledCount = FailBoatCalledCount + 1;
                        throw new Exception();
                    }
                    else if (amom is WorkFirstTime)
                    {
                        FirstTimeCalledCount = FirstTimeCalledCount + 1;
                        Deduplication.MarkCompletion(amom.TestKey);
                    }
                    else if (amom is WorkThirdTime)
                    {
                        ThirdTimeCalledCount = ThirdTimeCalledCount + 1;
                        if (state.NumberAttempts != 3)
                        {
                            throw new Exception();
                        }
                        Deduplication.MarkCompletion(amom.TestKey);
                    }
                }
                if (message is IConfirmable confirmable)
                {
                    Context.Sender.Tell(new DeduplicatedDeliveryConfirmation()
                    {
                        SequenceNr = confirmable.SequenceNr, Key = amom.TestKey
                    });
                }
            }

            

            return true;
        }

        public static int ThirdTimeCalledCount;

        public static int FailBoatCalledCount;

        public override DeduplicationPruningConfiguration PruningConfiguration
        {
            get;
        }

        public override string ReplicatorKey { get; }
        public override int MaxAttempts { get; }
    }
}