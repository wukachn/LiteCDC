CREATE DATABASE IF NOT EXISTS cdc_public;
GRANT ALL PRIVILEGES ON cdc_public.* TO 'mysql_user'@'%';

CREATE TABLE IF NOT EXISTS cdc_public.newtable1 (position INT PRIMARY KEY, name VARCHAR(16), cdc_last_updated INT);
CREATE TABLE IF NOT EXISTS cdc_public.newtable2 (position INT PRIMARY KEY, name VARCHAR(16), cdc_last_updated INT);