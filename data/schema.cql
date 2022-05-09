//Create keyspace
CREATE KEYSPACE IF NOT EXISTS TemperatureKeySpace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

//Create table
CREATE TABLE TemperatureKeySpace.Temperature (id text , timeStamp timestamp, value double, PRIMARY KEY (id));