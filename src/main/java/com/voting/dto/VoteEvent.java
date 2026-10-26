package com.voting.dto;

import java.time.Instant;

/** Immutable event published to Kafka. eventId is the idempotency key for the DB writer. */
public record VoteEvent(String eventId, String pollId, String userId, String optionId, Instant castAt) {}
