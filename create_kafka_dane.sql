CREATE TABLE IF NOT EXISTS kafka_dane (
    event_time     TIMESTAMP,
    event_type     VARCHAR(50),
    product_id     BIGINT,
    category_id    BIGINT,
    category_code  VARCHAR(255),
    brand          VARCHAR(100),
    price          NUMERIC(10, 2),
    user_id        BIGINT,
    user_session   UUID
);
