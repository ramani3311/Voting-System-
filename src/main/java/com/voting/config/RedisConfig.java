package com.voting.config;

import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.script.DefaultRedisScript;

@Configuration
public class RedisConfig {

    /** Atomic "check-and-vote". Redis executes a Lua script on its single command thread, so
     *  nothing can interleave between the SET NX and the HINCRBY: no race, no double count.
     *  KEYS[1]=voted:{poll}:{user}  KEYS[2]=counts:{poll}  ARGV[1]=ttlSeconds  ARGV[2]=option
     *  returns 1 = accepted, 0 = duplicate. */
    @Bean
    DefaultRedisScript<Long> castVoteScript() {
        return new DefaultRedisScript<>("""
            if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then
              redis.call('HINCRBY', KEYS[2], ARGV[2], 1)
              return 1
            end
            return 0
            """, Long.class);
    }

    /** Compensation if the Kafka publish fails after the vote was accepted. */
    @Bean
    DefaultRedisScript<Long> rollbackVoteScript() {
        return new DefaultRedisScript<>("""
            if redis.call('DEL', KEYS[1]) == 1 then
              redis.call('HINCRBY', KEYS[2], ARGV[1], -1)
              return 1
            end
            return 0
            """, Long.class);
    }

    public static String votedKey(String pollId, String userId) { return "voted:" + pollId + ":" + userId; }
    public static String countsKey(String pollId) { return "counts:" + pollId; }
    public static List<String> keys(String a, String b) { return List.of(a, b); }
}
