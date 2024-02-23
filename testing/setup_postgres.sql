CREATE TABLE IF NOT EXISTS newtable1 (position INT PRIMARY KEY, name VARCHAR(16));
CREATE TABLE IF NOT EXISTS newtable2 (position INT PRIMARY KEY, name VARCHAR(16));
CREATE TABLE Towns (
  id SERIAL UNIQUE NOT NULL,
  code VARCHAR(10) NOT NULL,
  article TEXT,
  name TEXT NOT NULL,
  department VARCHAR(4) NOT NULL,
  UNIQUE (code, department)
);

DO $$
DECLARE
    table_record RECORD;
BEGIN
    FOR table_record IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    LOOP
        EXECUTE 'ALTER TABLE ' || table_record.table_name || ' REPLICA IDENTITY FULL;';
    END LOOP;
END $$;

INSERT INTO newtable1 VALUES (1, 'one');
INSERT INTO newtable1 VALUES (2, 'two');
INSERT INTO newtable1 VALUES (3, 'three');

INSERT INTO newtable2 VALUES (100, 'one hundo');
INSERT INTO newtable2 VALUES (200, 'two hundo');
INSERT INTO newtable2 VALUES (300, 'three hundo');
INSERT INTO newtable2 VALUES (400, 'four hundo');