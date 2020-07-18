namespace Akka.Deduplication.DData
{
    public enum DeduplicationProcessingState
    {
        NotAttempted,
        Attempted,
        Processed,
        Error
    }
}