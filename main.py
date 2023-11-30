#!/usr/bin/env python3
import os
import pydgraph
import logging
import random

from cassandra.cluster import Cluster

from dgraph_section import model as dgraph_model
from cassandra_section import model as cassandra_model
from fastapi import FastAPI
from mongo_section import model as mongo_model

app = FastAPI()

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
        for key in mm_options.keys():
            print(key, '--', mm_options[key])
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
            1: "Fill data",
            2: "Show recommended airports",
            3: "Show all airports",
            4: "Show passengers per airport",
            5: "Exit",
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

        option = 1

        while(option != 5):
            print_menu()
            option = int(input('Enter your choice: '))
            if option == 1:
                cassandra_model.insert_data(session)
            if option == 2:
                cassandra_model.get_airports_food_service(session)
            if option == 3:
                cassandra_model.get_all_airports_passengers(session)
            if option == 4:
                cassandra_model.get_passengers_per_airport(session)
    
    main_cassandra()

def mongo_menu():
    mom_options = {
        1: "Recomendacion de mes por aeropuerto",
        2: "Mostrar todos los aeropuertos",
        3: "Visitantes durante el a año de un aeropuerto",
        4: "Exit",
    }
    
    for key in mom_options.keys():
        print(key, '--', mom_options[key])
    option = int(input("Select an option: "))
    while option != 4:
        if option == 1:
            mongo_model.get_recomendations_all()
        if option == 2:
            mongo_model.get_all_months_airports()
        if option == 3:
            mongo_model.visitors_through_year_in_airport()

        for key in mom_options.keys():
            print(key, '--', mom_options[key])
        option = int(input("Select an option: "))

def dgraph_menu():
   ## DGraph

    DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

    def print_menu():
        mm_options = {
            1: "Create data",
            2: "Search person by age",
            3: "Delete person by id",
            4: "Drop All",
            5: "Search Airline flights",
            6: "Search all airlines",
            7: "Exit"
        }
        for key in mm_options.keys():
            print(key, '--', mm_options[key])


    def create_client_stub():
        return pydgraph.DgraphClientStub(DGRAPH_URI)


    def create_client(client_stub):
        return pydgraph.DgraphClient(client_stub)


    def close_client_stub(client_stub):
        client_stub.close()


    def main_dgraph():
        # Init Client Stub and Dgraph Client
        client_stub = create_client_stub()
        client = create_client(client_stub)

        # Create schema
        dgraph_model.set_schema(client)
        option = 1
        while(option != 7):
            print_menu()
            option = int(input('Enter your choice: '))
            if option == 1:
                dgraph_model.create_data(client)
            if option == 2:
                person = input("Introduce age: ")
                dgraph_model.search_person(client, person)
            if option == 3:
                person = input("Introduce id: ")
                dgraph_model.delete_person(client, person)
            if option == 6:
                first_month = input("Introduce first month: ")
                second_month = input("Introduce second month: ")
                dgraph_model.buscarTodos(client, first_month, second_month)
            if option == 4:
                dgraph_model.drop_all(client)
            if option == 5:
                person = input("Introduce airline: ")
                dgraph_model.buscarAirline(client, person)
        dgraph_model.drop_all(client)
        close_client_stub(client_stub)
                
    main_dgraph()

if __name__ == '__main__':
    main_menu()
