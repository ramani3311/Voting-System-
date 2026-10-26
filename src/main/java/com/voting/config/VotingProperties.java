package com.voting.config;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "voting")
public record VotingProperties(Duration voteTtl, String topic, int partitions, List<Poll> polls) {

    public record Poll(String id, String title, List<String> options) {}

    public Optional<Poll> find(String pollId) {
        return polls.stream().filter(p -> p.id().equals(pollId)).findFirst();
    }
}
