-- SWH DB schema upgrade
-- from_version: 2
-- to_version: 3
-- description: Drop throttling tables

DROP TABLE IF EXISTS t_read;
DROP TABLE IF EXISTS t_write;
