CREATE TABLE intake (
    sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    aggregate_id varchar(128) NOT NULL,
    content text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT current_timestamp,
    published_at timestamptz,
    partition_id integer,
    kafka_offset bigint
);
CREATE INDEX intake_pending ON intake(sequence) WHERE published_at IS NULL;
CREATE TABLE projection_versions (
    version varchar(48) PRIMARY KEY CHECK (version ~ '^[a-zA-Z0-9_-]+$'),
    created_at timestamptz NOT NULL DEFAULT current_timestamp
);
INSERT INTO projection_versions(version) VALUES ('v1');
CREATE TABLE active_projection (singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), version varchar(48) NOT NULL REFERENCES projection_versions);
INSERT INTO active_projection VALUES (true, 'v1');
CREATE TABLE processed_events (
    version varchar(48) NOT NULL REFERENCES projection_versions,
    consumer_name varchar(32) NOT NULL,
    event_id uuid NOT NULL,
    content text NOT NULL,
    PRIMARY KEY(version, consumer_name, event_id)
);
CREATE TABLE order_state (
    version varchar(48) NOT NULL REFERENCES projection_versions,
    order_id varchar(128) NOT NULL,
    status varchar(16) NOT NULL CHECK(status IN ('CREATED','PAID','CANCELLED','SHIPPED','DELIVERED')),
    last_event_id uuid NOT NULL,
    event_time timestamptz NOT NULL,
    channel varchar(64) NOT NULL,
    PRIMARY KEY(version, order_id)
);
CREATE TABLE analytics (
    version varchar(48) NOT NULL REFERENCES projection_versions,
    event_type varchar(32) NOT NULL,
    currency varchar(3) NOT NULL,
    event_count bigint NOT NULL CHECK(event_count >= 0),
    revenue numeric(24,2) NOT NULL,
    PRIMARY KEY(version, event_type, currency)
);
CREATE TABLE quarantine (
    version varchar(48) NOT NULL REFERENCES projection_versions,
    consumer_name varchar(32) NOT NULL,
    event_id uuid NOT NULL,
    category varchar(64) NOT NULL,
    topic varchar(128) NOT NULL,
    partition_id integer NOT NULL,
    kafka_offset bigint NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY(version, consumer_name, event_id, topic, partition_id, kafka_offset)
);
