-- =============================================================================
-- referral-engine  •  migration 001  •  initial schema
-- =============================================================================
-- Table prefix: re_  (avoids collisions with your application tables)
-- All monetary amounts: NUMERIC(20, 6)
-- Timestamps: TIMESTAMPTZ (always UTC)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Users
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS re_users (
    id                  BIGSERIAL       PRIMARY KEY,
    external_id         VARCHAR(255)    NOT NULL,
    has_active_deposit  BOOLEAN         NOT NULL DEFAULT FALSE,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_re_users_external_id UNIQUE (external_id)
);

CREATE INDEX IF NOT EXISTS idx_re_users_external_id
    ON re_users (external_id);

CREATE INDEX IF NOT EXISTS idx_re_users_deposit
    ON re_users (has_active_deposit)
    WHERE has_active_deposit = TRUE;

-- ---------------------------------------------------------------------------
-- Referral links  (closure-table style)
-- ---------------------------------------------------------------------------
-- level = 1  →  direct parent
-- level = N  →  N-th ancestor (populated by rebuild_tree)
--
-- UNIQUE (user_id, level) enables idempotent upserts during rebuild.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS re_referrals (
    id          BIGSERIAL   PRIMARY KEY,
    user_id     BIGINT      NOT NULL REFERENCES re_users(id) ON DELETE CASCADE,
    referrer_id BIGINT      NOT NULL REFERENCES re_users(id) ON DELETE CASCADE,
    level       SMALLINT    NOT NULL CHECK (level BETWEEN 1 AND 50),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_re_referrals_user_level UNIQUE (user_id, level),
    CONSTRAINT chk_re_referrals_no_self   CHECK (user_id <> referrer_id)
);

CREATE INDEX IF NOT EXISTS idx_re_referrals_user_id
    ON re_referrals (user_id);

CREATE INDEX IF NOT EXISTS idx_re_referrals_referrer_level
    ON re_referrals (referrer_id, level);

-- ---------------------------------------------------------------------------
-- Accrual records  (immutable audit log)
-- ---------------------------------------------------------------------------
-- source_key + recipient_user_id + level  →  uniqueness / idempotency key
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS re_accruals (
    id                  BIGSERIAL       PRIMARY KEY,
    source_user_id      BIGINT          NOT NULL REFERENCES re_users(id),
    recipient_user_id   BIGINT          NOT NULL REFERENCES re_users(id),
    level               SMALLINT        NOT NULL,
    base_amount         NUMERIC(20, 6)  NOT NULL CHECK (base_amount > 0),
    accrual_rate        NUMERIC(6, 4)   NOT NULL,
    accrual_amount      NUMERIC(20, 6)  NOT NULL CHECK (accrual_amount > 0),
    source_key          VARCHAR(255)    NOT NULL,
    source_tag          VARCHAR(255)    NOT NULL DEFAULT '',
    accrued_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_re_accruals_key
        UNIQUE (source_key, recipient_user_id, level)
);

-- Fast lookup: "what did user X earn, newest first?"
CREATE INDEX IF NOT EXISTS idx_re_accruals_recipient
    ON re_accruals (recipient_user_id, accrued_at DESC);

-- Fast lookup: "what did source user X generate?"
CREATE INDEX IF NOT EXISTS idx_re_accruals_source
    ON re_accruals (source_user_id);

-- Idempotency check
CREATE INDEX IF NOT EXISTS idx_re_accruals_key
    ON re_accruals (source_key);

-- ---------------------------------------------------------------------------
-- Leader bonus registry  (one-time awards)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS re_leader_awards (
    id          BIGSERIAL       PRIMARY KEY,
    user_id     BIGINT          NOT NULL REFERENCES re_users(id),
    level       SMALLINT        NOT NULL,
    bonus       NUMERIC(20, 6)  NOT NULL,
    awarded_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_re_leader_awards UNIQUE (user_id, level)
);

CREATE INDEX IF NOT EXISTS idx_re_leader_awards_user
    ON re_leader_awards (user_id);
