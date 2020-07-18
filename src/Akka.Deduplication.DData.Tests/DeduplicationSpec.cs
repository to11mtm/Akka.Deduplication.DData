using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Akka.Actor;
using Akka.Configuration;
using Akka.DistributedData;
using Akka.Persistence;
using Xunit;

namespace Akka.Deduplication.DData.Tests
{
    

    public class DeduplicationSpec
    {
        static Config config = ConfigurationFactory.ParseString(@"
                akka.actor.provider = cluster
                akka.loglevel = INFO
                akka.log-dead-letters-during-shutdown = off
            ").WithFallback(DistributedData.DistributedData.DefaultConfig());

        private static ActorSystem _system = ActorSystem.Create("ddata-test", config);
        public DeduplicationSpec()
        {
            
        }

        [Fact]
        public void DeduplicatingActorBase_Purges_on_Purge()
        {
            var testActor =
                _system.ActorOf(Props.Create(() =>
                    new SampleDeduplicatingActor("testALOD", 5)));
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
             

            var testActor = _system.ActorOf(Props.Create(() => new SampleDeduplicatingActor("testKey", 5)),"processTest");
            testActor.Tell(new WorkFirstTime(){TestKey = "DeDuplicatingActorBase_Stores_State_And_Sets_Processed"});
            testActor.Tell(new WorkFirstTime(){TestKey = "DeDuplicatingActorBase_Stores_State_And_Sets_Processed"});
            var result = testActor.Ask<PingPong>(new PingPong()).Result;
            Assert.Equal(1, SampleDeduplicatingActor.FirstTimeCalledCount);
        }
        
        [Fact]
        public void DeDuplicatingActorBase_Handles_Retries()
        {
            

            var testActor = _system.ActorOf(Props.Create(() => new SampleDeduplicatingActor("testKeyRetry", 5)),"retryTest");
            testActor.Tell(new WorkThirdTime(){TestKey = "DeDuplicatingActorBase_Handles_Retries"});
            testActor.Tell(new WorkThirdTime(){TestKey = "DeDuplicatingActorBase_Handles_Retries"});
            testActor.Tell(new WorkThirdTime(){TestKey = "DeDuplicatingActorBase_Handles_Retries"});
            testActor.Tell(new WorkThirdTime(){TestKey = "DeDuplicatingActorBase_Handles_Retries"});
            testActor.Tell(new WorkThirdTime(){TestKey = "DeDuplicatingActorBase_Handles_Retries"});
            var result = testActor.Ask<PingPong>(new PingPong()).Result;
            Assert.Equal(3, SampleDeduplicatingActor.ThirdTimeCalledCount);
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
            

            var testActor = _system.ActorOf(Props.Create(() => new SampleDeduplicatingActor("testKeyError", 5)),"errorTest");
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
            Assert.Equal(5, SampleDeduplicatingActor.FailBoatCalledCount);
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