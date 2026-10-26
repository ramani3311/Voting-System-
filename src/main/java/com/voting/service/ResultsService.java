package com.voting.service;

import static com.voting.config.RedisConfig.countsKey;

import com.voting.config.VotingProperties;
import com.voting.dto.ResultsResponse;
import java.util.List;
import java.util.Map;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class ResultsService {

    private final StringRedisTemplate redis;

    public ResultsService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** Live results come straight from Redis (O(options)), never from Postgres. */
    public ResultsResponse results(VotingProperties.Poll poll) {
        Map<Object, Object> raw = redis.opsForHash().entries(countsKey(poll.id()));
        List<ResultsResponse.Count> counts = poll.options().stream()
                .map(o -> new ResultsResponse.Count(o, parse(raw.get(o))))
                .toList();
        long total = counts.stream().mapToLong(ResultsResponse.Count::votes).sum();
        return new ResultsResponse(poll.id(), poll.title(), total, counts);
    }

    private static long parse(Object v) {
        return v == null ? 0L : Long.parseLong(v.toString());
    }
}
