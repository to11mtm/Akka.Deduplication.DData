using Akka.Actor;
using Akka.Persistence;

namespace Akka.Deduplication.DData.Tests
{
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
}