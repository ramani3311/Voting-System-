package com.voting.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConfig {

    /** Messages are keyed by userId (see VoteService). Keying by pollId would put a single
     *  poll's whole traffic on one partition; userId spreads it across all 6 partitions so
     *  the 3 consumer threads run in parallel. */
    @Bean
    NewTopic votesTopic(VotingProperties props) {
        return TopicBuilder.name(props.topic()).partitions(props.partitions()).replicas(1).build();
    }

    /** Whole-batch retry with exponential backoff. Safe because the DB write is idempotent
     *  (vote_ledger primary key). */
    @Bean
    CommonErrorHandler kafkaErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(500, 2.0);
        backOff.setMaxElapsedTime(30_000);
        return new DefaultErrorHandler(backOff);
    }
}
