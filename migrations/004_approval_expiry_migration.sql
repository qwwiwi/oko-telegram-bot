-- Pending approvals expire; supergroup migration links the old ID to the new one.
-- Additive only: no existing data is dropped.
ALTER TABLE chat_approvals ADD COLUMN IF NOT EXISTS migrated_to bigint;
ALTER TABLE chat_approvals DROP CONSTRAINT IF EXISTS chat_approvals_status_check;
ALTER TABLE chat_approvals ADD CONSTRAINT chat_approvals_status_check
    CHECK(status IN ('pending','active','declined','stopped','removed','expired'));
INSERT INTO schema_version(version) VALUES(4) ON CONFLICT DO NOTHING;
