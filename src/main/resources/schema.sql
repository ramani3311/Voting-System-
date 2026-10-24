CREATE TABLE IF NOT EXISTS poll_results (
    poll_id   VARCHAR(64) NOT NULL,
    option_id VARCHAR(64) NOT NULL,
    votes     BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (poll_id, option_id)
);

-- Ledger gives the consumer exactly-once *effect* on top of Kafka's at-least-once delivery.
CREATE TABLE IF NOT EXISTS vote_ledger (
    event_id    VARCHAR(36) PRIMARY KEY,
    poll_id     VARCHAR(64) NOT NULL,
    option_id   VARCHAR(64) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
