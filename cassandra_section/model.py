#!/usr/bin/env python3
import logging

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_FLIGHTS_DATA_TABLE = """
    CREATE TABLE IF NOT EXISTS flights_data (
        passenger_id int,
        airline text,
        origin text,
        destination text,
        day varint,
        month text,
        year varint,
        age varint,
        gender text,
        reason text,
        stay text,
        connection boolean,
        transit text,
        wait varint,
        PRIMARY KEY (destination, connection, wait)
    ) with clustering order by (connection ASC, wait ASC);
"""

CREATE_DESTINATION_COUNTS = """
    CREATE TABLE IF NOT EXISTS destination_counts (
        destination text PRIMARY KEY,
        passengers int
    );
"""

SELECT_AIRPORTS_FOOD_SERVICE = """
    SELECT destination
    FROM destination_counts
    WHERE passengers > 3
    ALLOW FILTERING;
"""

SELECT_ALL_AIRPORTS_PASSENGERS = """
    SELECT destination, COUNT(*) as passengers
    FROM flights_data
    WHERE connection = True and wait > 60 and wait < 360
    GROUP BY destination
    ALLOW FILTERING;
"""


def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_FLIGHTS_DATA_TABLE)
    session.execute(CREATE_DESTINATION_COUNTS)


def get_airports_food_service(session):
    log.info("Retrieving airports with food service")
    stmt = session.prepare(SELECT_AIRPORTS_FOOD_SERVICE)
    rows = session.execute(stmt)
    for row in rows:
        print(f"=== Airport: {row.destination} ===")
        

def get_all_airports_passengers(session):
    log.info("Retrieving all airports with passengers")
    stmt = session.prepare(SELECT_ALL_AIRPORTS_PASSENGERS)
    rows = session.execute(stmt)
    for row in rows:
        print(f"=== Airport: {row.destination} ===")
        print(f"- # of Passengers: {row.passengers}")


