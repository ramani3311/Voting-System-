package com.voting.service;

import static com.voting.config.RedisConfig.countsKey;
import static com.voting.config.RedisConfig.keys;
import static com.voting.config.RedisConfig.votedKey;

import com.voting.config.VotingProperties;
import com.voting.dto.VoteEvent;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class VoteService {

    public enum Outcome { ACCEPTED, DUPLICATE }
    public record Result(Outcome outcome, String eventId) {}

    private static final Logger log = LoggerFactory.getLogger(VoteService.class);

    private final StringRedisTemplate redis;
    private final KafkaTemplate<String, VoteEvent> kafka;
    private final RedisScript<Long> castVote;
    private final RedisScript<Long> rollback;
    private final VotingProperties props;

    public VoteService(StringRedisTemplate redis, KafkaTemplate<String, VoteEvent> kafka,
                       RedisScript<Long> castVoteScript, RedisScript<Long> rollbackVoteScript,
                       VotingProperties props) {
        this.redis = redis;
        this.kafka = kafka;
        this.castVote = castVoteScript;
        this.rollback = rollbackVoteScript;
        this.props = props;
    }

    /** Hot path: one atomic Redis round-trip + a non-blocking Kafka send. */
    public Result cast(String pollId, String userId, String optionId) {
        String voted = votedKey(pollId, userId);
        String counts = countsKey(pollId);

        Long ok = redis.execute(castVote, keys(voted, counts),
                String.valueOf(props.voteTtl().toSeconds()), optionId);
        if (ok == null || ok == 0L) {
            return new Result(Outcome.DUPLICATE, null);
        }

        VoteEvent event = new VoteEvent(UUID.randomUUID().toString(), pollId, userId, optionId, Instant.now());

        // Async send: the request thread is NOT blocked waiting for the broker ack.
        kafka.send(props.topic(), userId, event).whenComplete((meta, err) -> {
            if (err != null) {
                log.error("Kafka publish failed for event {}, rolling back", event.eventId(), err);
                redis.execute(rollback, keys(voted, counts), optionId);
            }
        });
        return new Result(Outcome.ACCEPTED, event.eventId());
    }
}
