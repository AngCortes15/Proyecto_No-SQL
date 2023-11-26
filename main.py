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
    while option != 4:
        if option == 1:
            cassandra_menu()
        if option == 2:
            mongo_menu()
        if option == 3:
            dgraph_menu()
        option = int(input("Select an option: "))

def cassandra_menu():
    ## Cassandr
    # Set logger
    log = logging.getLogger()
    log.setLevel('INFO')
    handler = logging.FileHandler('cassandra.log')
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    log.addHandler(handler)

    # Read env vars releated to Cassandra App
    CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
    KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'proyecto')
    REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


    def print_menu():
        mm_options = {
            1: "Show recommended airports",
            2: "Show all airports",
            3: "Exit",
        }
        for key in mm_options.keys():
            print(key, '--', mm_options[key])


    def main_cassandra():
        log.info("Connecting to Cluster")
        cluster = Cluster(CLUSTER_IPS.split(','))
        session = cluster.connect()

        cassandra_model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
        session.set_keyspace(KEYSPACE)

        cassandra_model.create_schema(session)


        while(True):
            print_menu()
            option = int(input('Enter your choice: '))
            if option == 1:
                cassandra_model.get_airports_food_service(session)
            if option == 2:
                cassandra_model.get_all_airports_passengers(session)
            if option == 3:
                exit(0)
    
    main_cassandra()


def mongo_menu():
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


def dgraph_menu():
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
    main_menu()
    #main()
