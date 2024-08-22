-- SWH DB schema upgrade
-- from_version: 1
-- to_version: 2
-- description: Add indexes on the throttler tables

CREATE INDEX IF NOT EXISTS t_read_updated ON t_read USING brin(updated);
CREATE INDEX IF NOT EXISTS t_write_updated ON t_write USING brin(updated);
