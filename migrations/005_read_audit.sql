-- One row per read-API request: no query strings, bodies, tokens or message text.
-- Additive only: no existing data is dropped. Retention: `oko audit prune`.
CREATE TABLE IF NOT EXISTS read_audit (
    id bigserial PRIMARY KEY,
    at timestamptz NOT NULL DEFAULT now(),
    -- Configured agent name; NULL when the bearer token was missing or unknown.
    agent text,
    -- Route template such as /v1/chats/{chat_id}/messages, never the raw URL.
    route text NOT NULL,
    chat_id bigint,
    status smallint NOT NULL,
    result_count integer
);
CREATE INDEX IF NOT EXISTS read_audit_at ON read_audit(at);
CREATE INDEX IF NOT EXISTS read_audit_agent ON read_audit(agent,at);
INSERT INTO schema_version(version) VALUES(5) ON CONFLICT DO NOTHING;
