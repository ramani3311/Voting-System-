package com.voting.service;

import static com.voting.config.RedisConfig.countsKey;

import com.voting.config.VotingProperties;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** On startup, seed Redis counters from Postgres with HSETNX (never overwrites live counters).
 *  Gives the system a recovery path if Redis loses its data. */
@Component
public class CacheWarmer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CacheWarmer.class);

    private final StringRedisTemplate redis;
    private final JdbcTemplate jdbc;
    private final VotingProperties props;

    public CacheWarmer(StringRedisTemplate redis, JdbcTemplate jdbc, VotingProperties props) {
        this.redis = redis;
        this.jdbc = jdbc;
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) {
        for (VotingProperties.Poll poll : props.polls()) {
            Map<String, Long> persisted = new HashMap<>();
            jdbc.query("SELECT option_id, votes FROM poll_results WHERE poll_id = ?",
                    rs -> { persisted.put(rs.getString(1), rs.getLong(2)); }, poll.id());
            for (String option : poll.options()) {
                redis.opsForHash().putIfAbsent(countsKey(poll.id()), option,
                        String.valueOf(persisted.getOrDefault(option, 0L)));
            }
            log.info("Warmed Redis counters for {} from {} persisted rows", poll.id(), persisted.size());
        }
    }
}
