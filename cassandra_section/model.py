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

SELECT_AIRPORTS_FOOD_SERVICE = """
    SELECT destination, count(*) as passengers
    FROM flights_data
    WHERE connection = true AND wait > 60 and wait < 360
    GROUP BY destination
    ALLOW FILTERING;
"""


def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_FLIGHTS_DATA_TABLE)


def get_airports_food_service(session):
    log.info("Retrieving airports with food service")
    stmt = session.prepare(SELECT_AIRPORTS_FOOD_SERVICE)
    rows = session.execute(stmt)
    for row in rows:
        print(f"=== Airport: {row.destination} ===")
        print(f"- Connections: {row.destination_count}")


def get_trade_history_date(session, account, date1, date2):
    log.info(f"Retrieving {account} all trades between {date1} and {date2}")
    stmt = session.prepare(SELECT_TRADE_HISTORY_DATE_RANGE)
    rows = session.execute(stmt, [account, date1, date2])
    for row in rows:
        print(f"=== Account: {row.account} ===")
        print(f"- Trade ID: {row.trade_id}")
        print(f"- Type: {row.type}")
        print(f"- Symbol: {row.symbol}")
        print(f"- Shares: {row.shares}")
        print(f"- Price: {row.price}")
        print(f"- Amount: {row.amount}")

