-- Owner-managed chat approval gate. Additive only: no existing data is dropped.
CREATE TABLE IF NOT EXISTS chat_approvals (
    chat_id bigint PRIMARY KEY CHECK(chat_id <> 0),
    status text NOT NULL
        CHECK(status IN ('pending','active','declined','stopped','removed')),
    -- Owner-facing label from Telegram's chat title; never client message content.
    title text NOT NULL DEFAULT '' CHECK(length(title) <= 128),
    kind text,
    requested_at timestamptz NOT NULL DEFAULT now(),
    approved_at timestamptz,
    approved_by bigint,
    decided_by bigint,
    updated_at timestamptz NOT NULL DEFAULT now(),
    -- Count of updates acknowledged and dropped while not collecting (no content).
    dropped bigint NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS chat_approvals_status ON chat_approvals(status,requested_at);
INSERT INTO schema_version(version) VALUES(3) ON CONFLICT DO NOTHING;
