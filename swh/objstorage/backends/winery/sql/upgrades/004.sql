-- SWH DB schema upgrade
-- from_version: 3
-- to_version: 4
-- description: Ensure the pre-migration script has been executed.

DO $$
  BEGIN
    IF NOT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_name='shards' AND column_name='pool_name'
      ) THEN

        RAISE EXCEPTION 'Table "shards": missing column "pool_name"'
        USING HINT='Error: you must run the winery upgrade command';
      END IF;
  END;
$$ LANGUAGE plpgsql;
