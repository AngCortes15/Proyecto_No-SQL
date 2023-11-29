# Cassandra

"""
1.- Create Casssandra Cluster in Docker
2.- Create Keyspace 
3.- Create table : CREATE TABLE flights (passenger_id int, airline text, origin text, destination text, day varint,
 month text, year varint, age varint, gender text, reason text, stay text, connection boolean, transit text, wait varint, PRIMARY KEY (passenger_id, airline, month, day))
 with clustering order by (airline ASC, month ASC, day ASC);
4.- Copy data from host to Docker: docker cp flights.csv cassandra:/home/flights.csv
5.- Insert data: COPY flights FROM '/home/flights.csv' WITH HEADER=TRUE;

"""

# MongoDB
"""
1.- Create MongoDB Container in Docker
2.- Create Database iteso; use iteso
3.- Create Collection: db.createCollection("airports")
4.- Copy flights.csv from host to container: docker cp flights.csv mongo:/flights.csv
5.- Conect to mongo container: docker exec -it mongo bash
6.- mongoimport --type csv --headerline --db iteso --collection airports --file /flights.csv
"""

# DQL
"""
1.- Create Dgraph Container in Docker
"""