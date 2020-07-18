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

    public class PurgeEntry
    {
        public string Key { get; set; }
    }

    public interface IConfirmable
    {
        long SequenceNr { get; }
    }
}