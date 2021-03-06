﻿using System;

namespace Akka.Deduplication.DData
{
    public class DeduplicationState
    {
        public int NumberAttempts { get; set; }
        public DeduplicationProcessingState ProcessingState { get; set; }
        public DateTime? PruneAfter { get; set; }
    }
}