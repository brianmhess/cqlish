CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
CREATE TABLE IF NOT EXISTS test.itest(pkey INT, ccol INT, x INT, y INT, PRIMARY KEY ((pkey), ccol));
INSERT INTO test.itest(pkey,ccol,x,y) VALUES (1,1,1,1);
INSERT INTO test.itest(pkey,ccol,x,y) VALUES (2,2,2,2);
INSERT INTO test.itest(pkey,ccol,x,y) VALUES (3,3,3,3);
INSERT INTO test.itest(pkey,ccol,x) VALUES (10,10,10);
INSERT INTO test.itest(pkey,ccol,y) VALUES (20,20,20);
