CREATE TABLE IF NOT EXISTS schema_version (version integer PRIMARY KEY);
CREATE TABLE poll_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    epoch bigint NOT NULL DEFAULT 0, next_offset bigint NOT NULL DEFAULT 0,
    sidecar_session text, last_commit timestamptz, last_poll timestamptz,
    failures integer NOT NULL DEFAULT 0
);
INSERT INTO poll_state(singleton) VALUES(true);
CREATE TABLE events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fingerprint text UNIQUE NOT NULL, update_id bigint NOT NULL,
    chat_id bigint, raw bytea, error_code text, received_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX events_chat ON events(chat_id,id);
CREATE TABLE mapping_failures (fingerprint text PRIMARY KEY, attempts integer NOT NULL);
CREATE TABLE messages (
    chat_id bigint NOT NULL, message_id bigint NOT NULL,
    event_id bigint NOT NULL REFERENCES events(id), revision_time bigint NOT NULL,
    sent_at timestamptz NOT NULL, author_id bigint, text text NOT NULL,
    media_group_id text, reply_to_id bigint, PRIMARY KEY(chat_id,message_id)
);
CREATE INDEX messages_date ON messages(chat_id,sent_at,message_id);
CREATE TABLE revisions (
    event_id bigint PRIMARY KEY REFERENCES events(id), chat_id bigint NOT NULL,
    message_id bigint NOT NULL, revision_time bigint NOT NULL, text text NOT NULL
);
CREATE TABLE attachments (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id bigint NOT NULL REFERENCES events(id), chat_id bigint NOT NULL,
    message_id bigint NOT NULL, slot text NOT NULL, file_id text NOT NULL,
    file_unique_id text, declared_size bigint, mime text, kind text NOT NULL,
    status text NOT NULL DEFAULT 'pending', error_code text,
    relative_path text, size bigint, sha256 text, UNIQUE(event_id,slot)
);
CREATE TABLE transcripts (
    attachment_id bigint PRIMARY KEY REFERENCES attachments(id), chat_id bigint NOT NULL,
    status text NOT NULL DEFAULT 'pending', provider text, submission_id text,
    uploaded_url text, text text, error_code text
);
CREATE TABLE jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dedup_key text UNIQUE NOT NULL, kind text NOT NULL, chat_id bigint,
    attachment_id bigint REFERENCES attachments(id), payload jsonb NOT NULL DEFAULT '{}',
    status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','running','done','failed')),
    attempts integer NOT NULL DEFAULT 0, next_run timestamptz NOT NULL DEFAULT now(),
    lease_token uuid, lease_until timestamptz, error_code text,
    expires_at timestamptz, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX jobs_pending ON jobs(status,next_run,kind);
CREATE TABLE unknown_chats (
    chat_id bigint PRIMARY KEY, kind text NOT NULL, expires_at timestamptz NOT NULL
);
CREATE TABLE enrollment_hints (
    old_chat_id bigint NOT NULL, new_chat_id bigint NOT NULL,
    PRIMARY KEY(old_chat_id,new_chat_id)
);
INSERT INTO schema_version(version) VALUES(1);
