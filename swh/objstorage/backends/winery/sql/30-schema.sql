CREATE TYPE shard_state AS ENUM (
    'standby',
    'writing',
    'full',
    'packing',
    'packed',
    'cleaning',
    'readonly'
);

CREATE TABLE shards (
    id bigserial PRIMARY KEY,
    state shard_state NOT NULL DEFAULT 'standby',
    locker_ts timestamptz,
    locker uuid,
    name char(32) NOT NULL UNIQUE,
    mapped_on_hosts_when_packed text[] NOT NULL DEFAULT '{}'
);

CREATE TYPE signature_state AS ENUM (
    'inflight',
    'present',
    'deleted'
);

CREATE TABLE signature2shard (
    signature bytea PRIMARY KEY,
    state signature_state NOT NULL DEFAULT 'inflight',
    shard bigint NOT NULL REFERENCES shards (id)
);

CREATE INDEX signature2shard_deleted ON signature2shard (signature, shard)
WHERE
    state = 'deleted';

CREATE INDEX signature2shard_shard_state ON signature2shard (shard, state);

CREATE TABLE t_read (
    id serial PRIMARY KEY,
    updated timestamp NOT NULL,
    bytes integer NOT NULL
);

CREATE TABLE t_write (
    id serial PRIMARY KEY,
    updated timestamp NOT NULL,
    bytes integer NOT NULL
);

CREATE TABLE shard_template (
    key BYTEA PRIMARY KEY,
    content bytea
)
WITH (
    autovacuum_enabled = FALSE
);
