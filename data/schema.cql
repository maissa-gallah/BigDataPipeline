//Create keyspace
CREATE KEYSPACE IF NOT EXISTS sensordatakeyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

//Create table
CREATE TABLE sensordatakeyspace.temperature (id text , timeStamp timestamp, value double, PRIMARY KEY (id));

CREATE TABLE sensordatakeyspace.humidity (id text , timeStamp timestamp, value double, PRIMARY KEY (id));

CREATE TABLE sensordatakeyspace.averagedata (id text , temperature double, humidity double, PRIMARY KEY (id));