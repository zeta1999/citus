CREATE TABLE dist_table (a int, b int, c text);
SELECT create_distributed_table('dist_table', 'a');

INSERT INTO dist_table VALUES (1, 6, 'txt1'), (2, 7, 'txt2'), (3, 8, 'txt3');


CREATE TABLE non_dist_1 (a int, b int, c text);
CREATE TABLE non_dist_2 (a int, c text);
CREATE TABLE non_dist_3 (a int);

INSERT INTO non_dist_1 SELECT * FROM dist_table;
INSERT INTO non_dist_2 SELECT a, c FROM dist_table;
INSERT INTO non_dist_3 SELECT a FROM dist_table;

TABLE non_dist_1;
TABLE non_dist_2;
TABLE non_dist_3;

TRUNCATE non_dist_1, non_dist_2, non_dist_3;

INSERT INTO non_dist_1 SELECT * FROM dist_table WHERE a = 1;
--INSERT INTO non_dist_2 SELECT a, c FROM dist_table WHERE a = 1;
INSERT INTO non_dist_3 SELECT a FROM dist_table WHERE a = 1;

TABLE non_dist_1;
TABLE non_dist_2;
TABLE non_dist_3;

TRUNCATE non_dist_1, non_dist_2, non_dist_3;


INSERT INTO non_dist_1(b, a, c) SELECT a, b, c FROM dist_table;

TABLE non_dist_1;

TRUNCATE non_dist_1;


EXPLAIN INSERT INTO non_dist_1 SELECT * FROM dist_table;
EXPLAIN INSERT INTO non_dist_1 SELECT * FROM dist_table WHERE a = 1;


INSERT INTO non_dist_1 SELECT * FROM dist_table RETURNING *;
INSERT INTO non_dist_1 SELECT * FROM dist_table WHERE a = 1 RETURNING *;


CREATE TABLE non_dist_unique (a int UNIQUE, b int);
INSERT INTO non_dist_unique SELECT a, b FROM dist_table;
TABLE non_dist_unique;
INSERT INTO non_dist_unique SELECT a+1, b FROM dist_table ON CONFLICT (a) DO NOTHING;
TABLE non_dist_unique;
INSERT INTO non_dist_unique SELECT a+1, b FROM dist_table ON CONFLICT (a) DO UPDATE SET a = -1;
TABLE non_dist_unique;