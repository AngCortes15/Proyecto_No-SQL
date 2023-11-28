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
from pymongo import MongoClient
import pprint

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

        option = 1

        while(option != 3):
            print_menu()
            option = int(input('Enter your choice: '))
            if option == 1:
                cassandra_model.get_airports_food_service(session)
            if option == 2:
                cassandra_model.get_all_airports_passengers(session)
    
    main_cassandra()


def mongo_menu():
    mom_options = {
        1: "Recomendacion de mes por aeropuerto",
        2: "Mostrar todos los aeropuertos",
        3: "Borrar datos",
        4: "Exit",
    }
    mongo_client = MongoClient("localhost:27017")
    db = mongo_client.iteso
    collection = db.airports
    months = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembe', 'Octubre', 'Noviembre', 'Diciembre']
    for key in mom_options.keys():
        print(key, '--', mom_options[key])
    option = int(input("Select an option: "))
    while option != 4:
        if option == 1:
            query = [
                {
                    "$group": {"_id": {"month": "$month", "destination": "$destination"}, "total": { "$sum": 1 } }
                },
                {
                    "$match": { "total": { "$gt": 4 } }
                }
            ]
            airports_months = {}
            for i in collection.aggregate(query):
                month = i['_id']['month']
                airport = i['_id']['destination']
                total = i['total']
                if airports_months.get(airport) != None:
                    airports_months[airport].append(month)
                else:
                    airports_months[airport] = [month]
            for j in airports_months:
                for x in airports_months[j]:
                    print(f'Para el aeropuerto {j} los meses recomendados son: {months[x]}')
        if option == 2:
            query = [
                {
                    "$group": {"_id": {"month": "$month", "destination": "$destination"}, "total": { "$sum": 1 } }
                }
            ]
            airports_months = {}
            for i in collection.aggregate(query):
                if len(i['_id']) != 0:
                    month = i['_id']['month']
                    airport = i['_id']['destination']
                    total = i['total']
                    if airports_months.get(airport) != None:
                        airports_months[airport].append(month)
                    else:
                        airports_months[airport] = [month]
            for j in airports_months:
                print(f'Para el aeropuerto {j} los meses con pasajeros son:', end= " ")
                for x in airports_months[j]:
                    print(f'{months[x - 1]} ', end= " ")
                print()
        if option == 3:
            collection.delete_many({})

        for key in mom_options.keys():
            print(key, '--', mom_options[key])
        option = int(input("Select an option: "))

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
