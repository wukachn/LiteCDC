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

CREATE TABLE types (
    position INT PRIMARY KEY,
    boolean_column BOOLEAN,
    smallint_column SMALLINT,
    integer_column INTEGER,
    bigint_column BIGINT,
    float_column REAL,
    double_column DOUBLE PRECISION,
    varchar_column varchar(10),
    other_column TEXT
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

INSERT INTO types (position, boolean_column, smallint_column, integer_column, bigint_column, float_column, double_column, varchar_column, other_column) VALUES
(1, TRUE, 100, 10000, 1000000000, 10.5, 20.5, 'varchar1', 'other_text1'),
(2, FALSE, 200, 20000, 2000000000, 20.5, 30.5, 'varchar2', 'other_text2'),
(3, TRUE, 300, 30000, 3000000000, 30.5, 40.5, 'varchar3', 'other_text3'),
(4, FALSE, 400, 40000, 4000000000, 40.5, 50.5, 'varchar4', 'other_text4'),
(5, TRUE, 500, 50000, 5000000000, 50.5, 60.5, 'varchar5', 'other_text5');