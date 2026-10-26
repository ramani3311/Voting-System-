package com.voting.service;

import com.voting.dto.VoteEvent;
import java.sql.PreparedStatement;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Receives up to 1000 votes per poll and collapses them into ONE SQL statement:
 * 10,000 votes/sec becomes roughly 10 DB statements/sec instead of 10,000.
 *
 * The statement is a single atomic CTE:
 *   1. insert events into vote_ledger, skipping event_ids already seen   (idempotency)
 *   2. group only the newly inserted rows by (poll, option)
 *   3. upsert  votes = votes + n                                          (aggregation)
 * Redelivered batches (rebalance, retry) therefore never double count.
 */
@Service
public class VoteBatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(VoteBatchConsumer.class);

    private static final String UPSERT_SQL = """
        WITH ins AS (
            INSERT INTO vote_ledger (event_id, poll_id, option_id)
            SELECT * FROM unnest(?::varchar[], ?::varchar[], ?::varchar[])
            ON CONFLICT (event_id) DO NOTHING
            RETURNING poll_id, option_id
        )
        INSERT INTO poll_results (poll_id, option_id, votes)
        SELECT poll_id, option_id, count(*) FROM ins GROUP BY poll_id, option_id
        ON CONFLICT (poll_id, option_id)
        DO UPDATE SET votes = poll_results.votes + EXCLUDED.votes
        """;

    private final JdbcTemplate jdbc;

    public VoteBatchConsumer(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @KafkaListener(topics = "${voting.topic}", id = "vote-batch-writer")
    @Transactional
    public void onBatch(List<ConsumerRecord<String, VoteEvent>> records) {
        if (records.isEmpty()) return;

        String[] ids = new String[records.size()];
        String[] polls = new String[records.size()];
        String[] options = new String[records.size()];
        for (int i = 0; i < records.size(); i++) {
            VoteEvent e = records.get(i).value();
            ids[i] = e.eventId();
            polls[i] = e.pollId();
            options[i] = e.optionId();
        }

        long start = System.nanoTime();
        jdbc.execute((ConnectionCallback<Integer>) con -> {
            try (PreparedStatement ps = con.prepareStatement(UPSERT_SQL)) {
                ps.setArray(1, con.createArrayOf("varchar", ids));
                ps.setArray(2, con.createArrayOf("varchar", polls));
                ps.setArray(3, con.createArrayOf("varchar", options));
                return ps.executeUpdate();
            }
        });
        log.debug("Persisted batch of {} votes in {} ms on {}", records.size(),
                (System.nanoTime() - start) / 1_000_000, Thread.currentThread().getName());
        // Offsets are committed by the container only after this method returns successfully.
    }
}
