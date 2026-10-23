"""初始数据结构：调度、运行、事件、资源、证据与配置。

字段与约束的唯一真源是 docs/03；SQL 里的枚举取值与 contracts/types.py 的状态枚举一致。
"""

from __future__ import annotations

import sqlite3

from storage.db.migrations.runner import execute_statements

NAME = "initial"

SCHEMA = """
CREATE TABLE job_templates (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    config TEXT NOT NULL DEFAULT '{}',
    config_schema_version TEXT NOT NULL,
    verification_policy TEXT NOT NULL,
    recovery_policy TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE schedules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('cron', 'at', 'interval')),
    expr TEXT,
    at TEXT,
    timezone TEXT NOT NULL,
    catch_up TEXT NOT NULL CHECK (catch_up IN ('run_once', 'skip', 'window')),
    window_minutes INTEGER NOT NULL,
    coalesce INTEGER NOT NULL CHECK (coalesce IN (0, 1)),
    priority INTEGER NOT NULL,
    deadline_minutes INTEGER NOT NULL,
    preheat_minutes INTEGER NOT NULL,
    keep_warm_minutes INTEGER NOT NULL,
    job_template_id TEXT NOT NULL REFERENCES job_templates (id),
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    next_run_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (window_minutes <= deadline_minutes)
);

CREATE TABLE job_runs (
    id TEXT PRIMARY KEY,
    template_id TEXT NOT NULL REFERENCES job_templates (id),
    schedule_id TEXT REFERENCES schedules (id),
    scheduled_at TEXT,
    created_at TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (
        state IN (
            'scheduled', 'queued', 'preparing', 'waiting', 'executing',
            'verifying', 'cleaning', 'recovering', 'succeeded', 'failed',
            'unknown', 'cancelled'
        )
    ),
    attempt INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0,
    deadline_at TEXT,
    started_at TEXT,
    finished_at TEXT,
    verdict_id TEXT REFERENCES verdicts (id),
    error_code TEXT,
    detail TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE verdicts (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES job_runs (id),
    status TEXT NOT NULL CHECK (status IN ('completed', 'failed', 'unknown')),
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    reason TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    checks TEXT NOT NULL DEFAULT '[]',
    evidence_ids TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL
);

CREATE TABLE request_idempotency (
    key TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES job_runs (id),
    created_at TEXT NOT NULL
);

CREATE TABLE run_phases (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES job_runs (id),
    phase TEXT NOT NULL CHECK (phase IN ('prepare', 'execute', 'verify', 'cleanup')),
    attempt INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'running', 'ok', 'failed', 'skipped', 'timed_out')
    ),
    started_at TEXT,
    finished_at TEXT,
    timeout_seconds INTEGER NOT NULL,
    detail TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE run_events (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES job_runs (id),
    seq INTEGER NOT NULL,
    type TEXT NOT NULL,
    ts TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    UNIQUE (run_id, seq)
);

CREATE TABLE events_global (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    ts TEXT NOT NULL
);

CREATE TABLE environments (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    name TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('unknown', 'off', 'booting', 'ready', 'busy', 'degraded', 'failed')
    ),
    last_probe_at TEXT,
    warm_until TEXT,
    detail TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE resources (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL CHECK (type IN ('environment', 'account', 'gpu', 'custom')),
    name TEXT NOT NULL,
    capacity INTEGER NOT NULL DEFAULT 1 CHECK (capacity >= 1),
    detail TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE resource_leases (
    id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources (id),
    run_id TEXT NOT NULL REFERENCES job_runs (id),
    owner TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('leased', 'expiring', 'released')),
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    renewed_at TEXT
);

CREATE TABLE plugins (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    api_version INTEGER NOT NULL,
    capabilities TEXT NOT NULL DEFAULT '[]',
    config_schema TEXT NOT NULL DEFAULT '{}',
    error_map TEXT NOT NULL DEFAULT '{}',
    requirements TEXT NOT NULL DEFAULT '{}',
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    installed_at TEXT NOT NULL
);

CREATE TABLE evidence (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES job_runs (id),
    kind TEXT NOT NULL CHECK (kind IN ('screenshot', 'log', 'probe', 'exit', 'artifact')),
    source TEXT NOT NULL CHECK (source IN ('kernel', 'plugin', 'host')),
    path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE config_revisions (
    id TEXT PRIMARY KEY,
    plugin_id TEXT NOT NULL,
    target TEXT NOT NULL,
    revision INTEGER NOT NULL,
    diff TEXT NOT NULL DEFAULT '{}',
    applied_at TEXT NOT NULL,
    result TEXT NOT NULL CHECK (result IN ('applied', 'rolled_back', 'failed')),
    UNIQUE (plugin_id, target, revision)
);

CREATE TABLE notifications (
    id TEXT PRIMARY KEY,
    run_id TEXT REFERENCES job_runs (id),
    channel TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    sent_at TEXT,
    error TEXT
);

CREATE TABLE advisor_calls (
    id TEXT PRIMARY KEY,
    run_id TEXT REFERENCES job_runs (id),
    purpose TEXT NOT NULL,
    model TEXT NOT NULL,
    tokens INTEGER NOT NULL DEFAULT 0,
    cost REAL NOT NULL DEFAULT 0,
    result TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_schedules_next_run ON schedules (enabled, next_run_at);
CREATE INDEX idx_job_runs_queue ON job_runs (state, priority DESC, scheduled_at, created_at);
CREATE INDEX idx_job_runs_schedule ON job_runs (schedule_id, scheduled_at);
CREATE INDEX idx_job_runs_template ON job_runs (template_id, scheduled_at);
CREATE INDEX idx_run_phases_run ON run_phases (run_id, attempt);
CREATE INDEX idx_run_events_run ON run_events (run_id, seq);
CREATE INDEX idx_resource_leases_resource ON resource_leases (resource_id, state);
CREATE INDEX idx_evidence_run ON evidence (run_id);
CREATE INDEX idx_verdicts_run ON verdicts (run_id);
CREATE INDEX idx_notifications_run ON notifications (run_id, status);
"""


def up(connection: sqlite3.Connection) -> None:
    execute_statements(connection, SCHEMA)
