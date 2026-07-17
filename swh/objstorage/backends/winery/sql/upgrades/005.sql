-- SWH DB schema upgrade
-- from_version: 4
-- to_version: 5
-- description: Add a 'importing' state

ALTER TYPE shard_state ADD VALUE 'importing';
