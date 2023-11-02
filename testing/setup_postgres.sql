CREATE TABLE IF NOT EXISTS newtable1 (position INT PRIMARY KEY, name VARCHAR(16));
CREATE TABLE IF NOT EXISTS newtable2 (position INT PRIMARY KEY, name VARCHAR(16));

INSERT INTO newtable1 VALUES (1, 'one');
INSERT INTO newtable1 VALUES (2, 'two');
INSERT INTO newtable1 VALUES (3, 'three');

INSERT INTO newtable2 VALUES (100, 'one hundo');
INSERT INTO newtable2 VALUES (200, 'two hundo');
INSERT INTO newtable2 VALUES (300, 'three hundo');
INSERT INTO newtable2 VALUES (400, 'four hundo');