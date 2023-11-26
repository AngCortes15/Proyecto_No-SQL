#!/usr/bin/env python3
import os
import pydgraph
import logging
import random

from cassandra.cluster import Cluster

from dgraph_section import model as dgraph_model
from cassandra_section import model as cassandra_model
from fastapi import FastAPI
from pymongo import MongoClient
from mongo_section import routes as mongo_routes

def main_menu():
    mm_options = {
        1: "Cassandra",
        2: "MongoDB",
        3: "DGraph",
        4: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])
    option = int(input("Select an option: "))
    return option

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
DB_NAME = os.getenv('MONGODB_DB_NAME', 'iteso')

app = FastAPI()

@app.on_event("startup")
def startup_db_client():
    app.mongodb_client = MongoClient(MONGODB_URI)
    app.database = app.mongodb_client[DB_NAME]
    print(f"Connected to MongoDB at: {MONGODB_URI} \n\t Database: {DB_NAME}")

@app.on_event("shutdown")
def shutdown_db_client():
    app.mongodb_client.close()
    print("Hasta la vista, baby!")

#app.include_router(book_router, tags=["books"], prefix="/book")

## DGraph

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

def print_menu():
    mm_options = {
        1: "Create data",
        2: "Search person",
        3: "Delete person",
        4: "Drop All",
        5: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def close_client_stub(client_stub):
    client_stub.close()


def main():
    # Init Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Create schema
    model.set_schema(client)
    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            model.create_data(client)
        if option == 2:
            person = input("Name: ")
            model.search_person(client, person)
        if option == 3:
            person = input("Name: ")
            model.delete_person(client, person)
        if option == 4:
            model.drop_all(client)
        if option == 5:
            model.drop_all(client)
            close_client_stub(client_stub)
            exit(0)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))


## Cassandra

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        1: "Show accounts",
        2: "Show positions",
        3: "Show trade history",
        4: "Change username",
        5: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def print_trade_history_menu():
    thm_options = {
        1: "All",
        2: "Date Range",
        3: "Transaction Type (Buy/Sell)",
        4: "Instrument Symbol",
    }
    for key in thm_options.keys():
        print('    ', key, '--', thm_options[key])


def set_username():
    username = input('**** Username to use app: ')
    log.info(f"Username set to {username}")
    return username


def get_instrument_value(instrument):
    instr_mock_sum = sum(bytearray(instrument, encoding='utf-8'))
    return random.uniform(1.0, instr_mock_sum)


def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)

    username = set_username()

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            model.get_user_accounts(session, username)
        if option == 2:
            account_number = input('Enter account number: ')
            model.get_user_positions(session, account_number)
        if option == 3:
            print_trade_history_menu()
            account_number = input('Enter account number: ')
            tv_option = int(input('Enter your trade view choice: '))
            if tv_option == 1:
                model.get_trade_history_all(session, account_number)
            if tv_option == 2:
                date1 = input('Enter Date 1: ')
                date2 = input('Enter Date 2: ')
                model.get_trade_history_date(session, account_number, date1, date2)
            if tv_option == 3:
                t = input('Enter transaction type (Buy/Sell): ')
                model.get_trade_history_type(session, account_number, t)
            if tv_option == 4:
                symbol = input('Enter instrument symbol: ')
                model.get_trade_history_symbol(session, account_number, symbol)
        if option == 4:
            username = set_username()
        if option == 5:
            exit(0)


if __name__ == '__main__':
    main_menu()
    #main()
