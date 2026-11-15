CREATE TABLE IF NOT EXISTS notification_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), last_attempt timestamptz
);
INSERT INTO notification_state(singleton) VALUES(true) ON CONFLICT DO NOTHING;
INSERT INTO schema_version(version) VALUES(2);
