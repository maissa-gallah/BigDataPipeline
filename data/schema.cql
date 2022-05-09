//Create keyspace
CREATE KEYSPACE IF NOT EXISTS temperaturekeyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

//Create table
CREATE TABLE temperaturekeyspace.temperature (id text , timeStamp timestamp, value double, PRIMARY KEY (id));