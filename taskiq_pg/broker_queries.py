"""SQL for the asyncpg broker. Status literals baked in; format {table_name} at call."""

from __future__ import annotations

from taskiq_pg.status import MessageStatus

# Additive DDL: base table + idempotent ALTERs so a legacy (master) table gains
# every new column in place before the indexes below reference them.
# lock_key stays vestigial (old advisory-lock workers still read it during rollout).
CREATE_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS {{table_name}} (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR NOT NULL,
    task_name VARCHAR NOT NULL,
    message TEXT NOT NULL,
    labels JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    scheduled_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status VARCHAR(20) DEFAULT '{MessageStatus.QUEUED.value}' CHECK (status IN ('{MessageStatus.QUEUED.value}', '{MessageStatus.ACTIVE.value}', '{MessageStatus.COMPLETED.value}', '{MessageStatus.DEAD.value}')),
    lock_key SERIAL NOT NULL,
    expire_at TIMESTAMP WITH TIME ZONE,
    group_key VARCHAR,
    retry_count INTEGER NOT NULL DEFAULT 0,
    heartbeat_at TIMESTAMP WITH TIME ZONE,
    ordered BOOLEAN NOT NULL DEFAULT FALSE
);
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT '{MessageStatus.QUEUED.value}';
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS lock_key SERIAL NOT NULL;
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS expire_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS group_key VARCHAR;
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMP WITH TIME ZONE;
-- Opt-in FIFO. Metadata-only on PG11+; existing rows read false, i.e. mutex-only.
ALTER TABLE {{table_name}} ADD COLUMN IF NOT EXISTS ordered BOOLEAN NOT NULL DEFAULT FALSE;
-- Legacy tables carry an auto-named CHECK without 'dead'. Only migrate when no
-- existing check constraint already permits 'dead' — DROP/ADD takes ACCESS EXCLUSIVE
-- and revalidates every row, so we must not run it on every startup.
DO $do$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = '{{table_name}}'::regclass
          AND contype = 'c'
          AND (SELECT attnum FROM pg_attribute
               WHERE attrelid = '{{table_name}}'::regclass
                 AND attname = 'status' AND NOT attisdropped) = ANY(conkey)
          AND pg_get_constraintdef(oid) LIKE '%dead%'
    ) THEN
        ALTER TABLE {{table_name}} DROP CONSTRAINT IF EXISTS {{table_name_safe}}_status_check;
        ALTER TABLE {{table_name}} ADD CONSTRAINT {{table_name_safe}}_status_check CHECK (status IN ('{MessageStatus.QUEUED.value}', '{MessageStatus.ACTIVE.value}', '{MessageStatus.COMPLETED.value}', '{MessageStatus.DEAD.value}'));
    END IF;
END
$do$;
-- Superseded by _scheduled_id: status is constant inside a partial index on status.
-- Matched by indrelid, not by name: a same-named index on another table or schema is
-- not ours to drop, and an unqualified DROP would resolve through search_path.
DO $do$
DECLARE
    victim oid;
BEGIN
    SELECT i.indexrelid INTO victim
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    WHERE i.indrelid = to_regclass('{{table_name}}')
      AND c.relname = 'idx_{{table_name_safe}}_status_scheduled';
    IF victim IS NOT NULL THEN
        EXECUTE format('DROP INDEX %s', victim::regclass);
    END IF;
END
$do$;
-- ORDER BY (scheduled_at, id) needs the tiebreak in the index or it sorts per call.
CREATE INDEX IF NOT EXISTS idx_{{table_name_safe}}_scheduled_id ON {{table_name}} (scheduled_at, id) WHERE status = '{MessageStatus.QUEUED.value}';
CREATE INDEX IF NOT EXISTS idx_{{table_name_safe}}_group_key ON {{table_name}} (group_key) WHERE group_key IS NOT NULL AND status = '{MessageStatus.ACTIVE.value}';
-- Serves both group probes: older unfinished rows, and dead rows that halt the group.
CREATE INDEX IF NOT EXISTS idx_{{table_name_safe}}_group_state ON {{table_name}} (group_key, status, id) WHERE group_key IS NOT NULL AND status IN ('{MessageStatus.QUEUED.value}', '{MessageStatus.ACTIVE.value}', '{MessageStatus.DEAD.value}');
CREATE INDEX IF NOT EXISTS idx_{{table_name_safe}}_expire_at ON {{table_name}} (expire_at) WHERE expire_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_{{table_name_safe}}_active_heartbeat ON {{table_name}} (heartbeat_at) WHERE status = '{MessageStatus.ACTIVE.value}';
"""  # noqa: E501

INSERT_MESSAGE_QUERY = """
INSERT INTO {table_name}
    (task_id, task_name, message, labels, group_key, ordered, expire_at, scheduled_at)
VALUES ($1, $2, $3, $4, $5, $6, NULL, {scheduled_at})
RETURNING id
"""

SELECT_MESSAGE_QUERY = "SELECT * FROM {table_name} WHERE id = $1"

DELETE_MESSAGE_QUERY = "DELETE FROM {table_name} WHERE id = $1"

# Claims one message, returns its id. $1 = advisory keyspace seed.
# Mutex re-checked after the advisory lock, as its own statement: plpgsql statements
# take a fresh snapshot each under READ COMMITTED, so it sees rivals' committed claims.
# Duplicate NOT EXISTS: different snapshots. Head-of-line and dead-halt (opt-in via
# `ordered`) need no re-check -- a stale read of either can only over-block.
# SKIP LOCKED fans workers out; cursor skips rejected candidates. BIGINT return, not
# SETOF table: composite type blocks DROP TABLE.
CLAIM_FUNCTION_QUERY = f"""
CREATE OR REPLACE FUNCTION {{claim_fn}}(p_keyspace BIGINT)
RETURNS BIGINT
LANGUAGE plpgsql AS $claim$
DECLARE
    cand RECORD;
    cur_sched TIMESTAMPTZ := '-infinity';
    cur_id BIGINT := 0;
BEGIN
    LOOP
        SELECT m.id, m.group_key, m.scheduled_at
        INTO cand
        FROM {{table_name}} m
        WHERE m.status = '{MessageStatus.QUEUED.value}'
          AND m.scheduled_at <= NOW()
          AND (m.expire_at IS NULL OR m.expire_at > NOW())
          AND (m.scheduled_at, m.id) > (cur_sched, cur_id)
          AND (m.group_key IS NULL OR (
              NOT EXISTS (
                  SELECT 1 FROM {{table_name}} a
                  WHERE a.group_key = m.group_key AND a.status = '{MessageStatus.ACTIVE.value}'
              )
              AND (NOT m.ordered OR (
                  NOT EXISTS (
                      SELECT 1 FROM {{table_name}} o
                      WHERE o.group_key = m.group_key AND o.id < m.id
                        AND o.status IN ('{MessageStatus.QUEUED.value}', '{MessageStatus.ACTIVE.value}')
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {{table_name}} d
                      WHERE d.group_key = m.group_key AND d.id < m.id
                        AND d.status = '{MessageStatus.DEAD.value}'
                  )
              ))
          ))
        ORDER BY m.scheduled_at, m.id
        LIMIT 1
        FOR UPDATE SKIP LOCKED;

        EXIT WHEN NOT FOUND;
        cur_sched := cand.scheduled_at;
        cur_id := cand.id;

        IF cand.group_key IS NOT NULL THEN
            IF NOT pg_try_advisory_xact_lock(hashtextextended(cand.group_key, p_keyspace)) THEN
                CONTINUE;
            END IF;
            IF EXISTS (
                SELECT 1 FROM {{table_name}} a
                WHERE a.group_key = cand.group_key AND a.status = '{MessageStatus.ACTIVE.value}'
            ) THEN
                CONTINUE;
            END IF;
        END IF;

        -- Row is ours via FOR UPDATE; status guard is the backstop.
        -- retry_count counts deliveries, so it is bumped here and nowhere else:
        -- whatever ended the previous attempt, crash or exception, costs the same.
        UPDATE {{table_name}}
        SET status = '{MessageStatus.ACTIVE.value}', heartbeat_at = NOW(),
            retry_count = retry_count + 1
        WHERE id = cand.id AND status = '{MessageStatus.QUEUED.value}';
        IF FOUND THEN
            RETURN cand.id;
        END IF;
    END LOOP;
    RETURN NULL;
END
$claim$;
"""  # noqa: E501

# Read in a LATER statement: an enclosing statement's snapshot predates the claim.
CLAIM_MESSAGE_QUERY = "SELECT {claim_fn}($1)"

# Hand a delivery back without moving it: same id, same place in its group. $2 is the
# attempt count the caller was given at claim; a worker whose row was reclaimed and
# handed to someone else sees a stale count and updates nothing. The next claim counts
# the new attempt, so this must not touch retry_count.
RETRY_IN_PLACE_QUERY = f"""
UPDATE {{table_name}}
SET status = '{MessageStatus.QUEUED.value}',
    scheduled_at = NOW() + ($3::DOUBLE PRECISION * INTERVAL '1 second'),
    heartbeat_at = NULL
WHERE id = $1
  AND status = '{MessageStatus.ACTIVE.value}'
  AND retry_count = $2::INTEGER
"""

# Terminal. Fenced like RETRY_IN_PLACE_QUERY.
MARK_DEAD_QUERY = f"""
UPDATE {{table_name}}
SET status = '{MessageStatus.DEAD.value}'
WHERE id = $1
  AND status = '{MessageStatus.ACTIVE.value}'
  AND retry_count = $2::INTEGER
"""

# Batched liveness refresh for this process's in-flight ids. status guard avoids
# resurrecting a row already swept back to queued.
HEARTBEAT_MESSAGES_QUERY = f"""
UPDATE {{table_name}}
SET heartbeat_at = NOW()
WHERE id = ANY($1::int[]) AND status = '{MessageStatus.ACTIVE.value}'
"""

# TTL = completed-retention window. $3 is the attempt count the caller was given at
# claim: only the delivery that owns the row may finish it, or a late ack from a
# reclaimed worker would complete whatever attempt holds the row now.
COMPLETE_MESSAGE_QUERY = f"""
UPDATE {{table_name}}
SET status = '{MessageStatus.COMPLETED.value}', expire_at = NOW() + ($1::INTEGER * INTERVAL '1 second')
WHERE id = $2 AND status = '{MessageStatus.ACTIVE.value}' AND retry_count = $3::INTEGER
"""  # noqa: E501

# Reclaim rows whose lease went stale (worker presumed dead). $1: timeout secs,
# $2: max_retry_attempts fallback. Reads the attempt count, never bumps it -- the
# claim does that. A row that has burned its attempts is parked in 'dead' (terminal,
# excluded from dequeue) instead of looping forever. A per-message `max_retries` label
# wins over $2; negative means retry forever. kick() validates it, so the cast is bare.
SWEEP_MESSAGES_QUERY = f"""
WITH stuck_messages AS (
    SELECT id
    FROM {{table_name}}
    WHERE status = '{MessageStatus.ACTIVE.value}'
      AND heartbeat_at < NOW() - ($1::INTEGER * INTERVAL '1 second')
    ORDER BY heartbeat_at
    LIMIT 100
    FOR UPDATE SKIP LOCKED
),
capped AS (
    SELECT m.id,
           coalesce((m.labels->>'max_retries')::INTEGER, $2::INTEGER) AS cap,
           m.retry_count
    FROM {{table_name}} m
    JOIN stuck_messages s ON s.id = m.id
)
UPDATE {{table_name}}
SET status = CASE
        WHEN capped.cap >= 0 AND capped.retry_count >= capped.cap
            THEN '{MessageStatus.DEAD.value}'
        ELSE '{MessageStatus.QUEUED.value}'
    END
FROM capped
WHERE {{table_name}}.id = capped.id
RETURNING {{table_name}}.id, {{table_name}}.status
"""

CLEANUP_EXPIRED_QUERY = f"""
DELETE FROM {{table_name}}
WHERE id IN (
    SELECT id
    FROM {{table_name}}
    WHERE expire_at IS NOT NULL
      AND expire_at < NOW()
      AND status = '{MessageStatus.COMPLETED.value}'
    LIMIT 1000
)
"""
