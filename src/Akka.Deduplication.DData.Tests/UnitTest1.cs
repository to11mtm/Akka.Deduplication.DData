using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Akka.Actor;
using Akka.Configuration;
using Akka.DistributedData;
using Akka.Event;
using Akka.Persistence;
using Xunit;

namespace Akka.Deduplication.DData.Tests
{
    public class DeduplicatedDeliveryConfirmation
    {
        public string Key { get; set; }
        public long SequenceNr { get; set; }
    }
    public interface IAtMostOnceMessage
    {
        string TestKey { get; }
    }
    public class FailBoat : IAtMostOnceMessage
    {
        public string TestKey { get; set; }
    }

    public class WorkFirstTime : IAtMostOnceMessage
    {
        public string TestKey { get; set; }
    }
    public class WorkFirstTimeWithDelay : IAtMostOnceMessage, IConfirmable
    {
        public string TestKey { get; set; }
        public long SequenceNr { get; set; }
    }

    public class PingPong
    {
        
    }
    public class WorkThirdTime : IAtMostOnceMessage
    {
        public string TestKey { get; set; }
    }

    public class TestCommand
    {
        public string Key { get; set; }
    }
    public class
        SampleAtLeastOnceDeduplicatingSender : AtLeastOnceDeliveryReceiveActor
    {
        public SampleAtLeastOnceDeduplicatingSender(string persistenceId, IActorRef recipient)
        {
            PersistenceId = persistenceId;
            
            
            Command<TestCommand>(m => Persist(m,
                evt => Deliver(recipient.Path,
                    s => new WorkFirstTimeWithDelay()
                        {SequenceNr = s, TestKey = evt.Key})));
            Command<DeduplicatedDeliveryConfirmation>(m => Persist(m,
                evt =>
                {
                    ConfirmDelivery(evt.SequenceNr);
                    recipient.Tell(new PurgeEntry(){Key = evt.Key});
                }));
        }
        public override string PersistenceId { get; }
    }

    public class PurgeEntry
    {
        public string Key { get; set; }
    }
    public class SampleRecieiver : DeduplicatingReceiverBase
    {
        public static int FirstTimeCalledCount = 0;
        public static int DelayCallCount = 0;
        public SampleRecieiver(string replicatorKey, int maxAttempts)
        {
            ReplicatorKey = replicatorKey;
            MaxAttempts = maxAttempts;
        }

        protected override void PreRestart(Exception reason, object message)
        {
            Context.GetLogger().Error(reason,"oops");
            base.PreRestart(reason, message);
        }

        protected override bool Receive(object message)
        {
            if (message is PurgeEntry purge)
            {
                this.PurgeKey(purge.Key);
            }
            if (message is PingPong)
            {
                Context.Sender.Tell(message);
            }
            if (message is IAtMostOnceMessage amom)
            {
                var state = this.AdvanceAndGetProcessingState(amom.TestKey);
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
                        this.MarkCompletion(amom.TestKey);
                    }
                    else if (amom is WorkThirdTime)
                    {
                        ThirdTimeCalledCount = ThirdTimeCalledCount + 1;
                        if (state.NumberAttempts != 3)
                        {
                            throw new Exception();
                        }
                        this.MarkCompletion(amom.TestKey);
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

        protected override string ReplicatorKey { get; }
        protected override int MaxAttempts { get; }
    }

    public interface IConfirmable
    {
        long SequenceNr { get; }
    }

    public class UnitTest1
    {
        static Config config = ConfigurationFactory.ParseString(@"
                akka.actor.provider = cluster
                akka.loglevel = INFO
                akka.log-dead-letters-during-shutdown = off
            ").WithFallback(DistributedData.DistributedData.DefaultConfig());

        private static ActorSystem _system = ActorSystem.Create("ddata-test", config);
        public UnitTest1()
        {
            
        }

        [Fact]
        public void DeduplicatingActorBase_Purges_on_Purge()
        {
            var testActor =
                _system.ActorOf(Props.Create(() =>
                    new SampleRecieiver("testALOD", 5)));
            var testAlodActor =
                _system.ActorOf(Props.Create(() =>
                    new SampleAtLeastOnceDeduplicatingSender("testALOD",
                        testActor)));
            testAlodActor.Tell(new TestCommand(){Key = "DeduplicatingActorBase_Purges_on_Purge"});
            SpinWait.SpinUntil(() => false, TimeSpan.FromSeconds(5));
            var waiter = testActor.Ask(new PingPong()).Result;
            var state = DistributedData.DistributedData.Get(_system)
                .GetAsync(
                    new LWWDictionaryKey<string, DeduplicationState>(
                        "testALOD")).Result;
            var record = state.FirstOrDefault(r =>
                r.Key == "DeduplicatingActorBase_Purges_on_Purge");
            Assert.Equal(default(KeyValuePair<string,DeduplicationState>),record);
        }
        [Fact]
        public void DeDuplicatingActorBase_Stores_State_And_Sets_Processed()
        {
             

            var testActor = _system.ActorOf(Props.Create(() => new SampleRecieiver("testKey", 5)),"processTest");
            testActor.Tell(new WorkFirstTime(){TestKey = "DeDuplicatingActorBase_Stores_State_And_Sets_Processed"});
            testActor.Tell(new WorkFirstTime(){TestKey = "DeDuplicatingActorBase_Stores_State_And_Sets_Processed"});
            var result = testActor.Ask<PingPong>(new PingPong()).Result;
            Assert.Equal(1, SampleRecieiver.FirstTimeCalledCount);
        }
        
        [Fact]
        public void DeDuplicatingActorBase_Handles_Retries()
        {
            

            var testActor = _system.ActorOf(Props.Create(() => new SampleRecieiver("testKeyRetry", 5)),"retryTest");
            testActor.Tell(new WorkThirdTime(){TestKey = "DeDuplicatingActorBase_Handles_Retries"});
            testActor.Tell(new WorkThirdTime(){TestKey = "DeDuplicatingActorBase_Handles_Retries"});
            testActor.Tell(new WorkThirdTime(){TestKey = "DeDuplicatingActorBase_Handles_Retries"});
            testActor.Tell(new WorkThirdTime(){TestKey = "DeDuplicatingActorBase_Handles_Retries"});
            testActor.Tell(new WorkThirdTime(){TestKey = "DeDuplicatingActorBase_Handles_Retries"});
            var result = testActor.Ask<PingPong>(new PingPong()).Result;
            Assert.Equal(3, SampleRecieiver.ThirdTimeCalledCount);
            var state = DistributedData.DistributedData.Get(_system)
                .GetAsync(
                    new LWWDictionaryKey<string, DeduplicationState>(
                        "testKeyRetry")).Result;
            var record = state.FirstOrDefault(r =>
                r.Key == "DeDuplicatingActorBase_Handles_Retries");
            Assert.Equal(DeduplicationProcessingState.Processed,record.Value.ProcessingState);
            Assert.Equal(3,record.Value.NumberAttempts);
        }
        
        [Fact]
        public void DeDuplicatingActorBase_Stops_Processing_After_Error()
        {
            

            var testActor = _system.ActorOf(Props.Create(() => new SampleRecieiver("testKeyError", 5)),"errorTest");
            testActor.Tell(new FailBoat(){TestKey = "DeDuplicatingActorBase_Stops_Processing_After_Error"});
            testActor.Tell(new FailBoat(){TestKey = "DeDuplicatingActorBase_Stops_Processing_After_Error"});
            testActor.Tell(new FailBoat(){TestKey = "DeDuplicatingActorBase_Stops_Processing_After_Error"});
            testActor.Tell(new FailBoat(){TestKey = "DeDuplicatingActorBase_Stops_Processing_After_Error"});
            testActor.Tell(new FailBoat(){TestKey = "DeDuplicatingActorBase_Stops_Processing_After_Error"});
            testActor.Tell(new FailBoat(){TestKey = "DeDuplicatingActorBase_Stops_Processing_After_Error"});
            testActor.Tell(new FailBoat(){TestKey = "DeDuplicatingActorBase_Stops_Processing_After_Error"});
            testActor.Tell(new FailBoat(){TestKey = "DeDuplicatingActorBase_Stops_Processing_After_Error"});
            testActor.Tell(new FailBoat(){TestKey = "DeDuplicatingActorBase_Stops_Processing_After_Error"});
            var result = testActor.Ask<PingPong>(new PingPong()).Result;
            Assert.Equal(5, SampleRecieiver.FailBoatCalledCount);
            var state = DistributedData.DistributedData.Get(_system)
                .GetAsync(
                    new LWWDictionaryKey<string, DeduplicationState>(
                        "testKeyError")).Result;
            var record = state.FirstOrDefault(r =>
                r.Key == "DeDuplicatingActorBase_Stops_Processing_After_Error");
            Assert.Equal(DeduplicationProcessingState.Error,record.Value.ProcessingState);
            Assert.Equal(5,record.Value.NumberAttempts);
        }


    }
}