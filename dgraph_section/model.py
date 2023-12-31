#!/usr/bin/env python3
import datetime
import pandas as pd
import json

import pydgraph

 

def set_schema(client):
    schema = """
    type Customer {
        age
        gender
        reason
        passenger_id
        flight
    }
    age: int @index(int) .
    gender: string @index(exact) .
    reason: string @index(exact) .
    passenger_id: int @index(int) .
    flight: [uid] @reverse .

    type Flight {
        airline
        origin
        destination
        day
        month
        year
        connection
        wait
    }
    airline: string @index(hash) .
    origin: string @index(exact) .
    destination: string .
    day: int @index(int) .
    month: int @index(int) . 
    year: int .
    connection: bool .
    wait: int .

    type Accommodation{
        stay
    }
    stay: string @index(hash) .

     type Transportation{
        transit
    }
    transit: string @index(exact) .
    """
     # type Transportation{
    #     transit
    # }
    # transit: string @index(exact) .
    return client.alter(pydgraph.Operation(schema=schema))


def create_data(client):
    # Create a new transaction.

    txn = client.txn()
    df = pd.read_csv('./flight_passengers.csv');
    dataset_list = df.to_dict(orient='records')

    try:
        mutations = []
        for i,data_point in enumerate(dataset_list):
            mutation = {
                'passenger_id' : data_point['passenger_id'],
                'uid': f'_:p{i+1}',  # Puedes generar un UID único para cada punto de datos si es necesario
                'dgraph.type': 'Customer',  # Asegúrate de que el tipo coincida con tu esquema
                'age': data_point['age'],
                'gender': data_point['gender'],
                'reason': data_point['reason'],     
                'flight': [{
                    'dgraph.type': 'Flight',
                    'airline': data_point['airline'],
                    'origin': data_point['origin'],
                    'destination': data_point['destination'],
                    'day': data_point['day'],
                    'month':  data_point['month'],
                    'year': data_point['year'],
                    'connection': data_point['connection'],
                    'wait': data_point['wait']
                    
                }],
                'accomodation': {
                    'dgraph.type': 'Accommodation',
                    'stay': data_point['stay']
                },
                'transportation': {
                    'dgraph.type': 'Transportation',
                    'transit': " "
                }
                # Agrega otras propiedades según tu esquema
            }
            if 'transit' in data_point and data_point['transit']:
                    mutation['transportation'] = {
                        'dgraph.type': 'Transportation',
                        'transit': ' '
                    }
            mutations.append(mutation)
        response = txn.mutate(set_obj=mutations)
        # print(mutations)
        # Commit transaction.
        commit_response = txn.commit()
        print(f"Commit Response: {commit_response}")

        print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


def delete_person(client, name):
    # Create a new transaction.
    txn = client.txn()
    try:
        query1 = """query search_person($a: int) {
            all(func: eq(passenger_id, $a)) {
               uid
               passenger_id
               age
               gender
               reason

            }
        }"""
        variables1 = {'$a': name}
        res1 = client.txn(read_only=True).query(query1, variables=variables1)
        ppl1 = json.loads(res1.json)
        for person in ppl1['all']:
            print("UID: " + person['uid'])
            txn.mutate(del_obj=person)
            print(f"{name} deleted")
        commit_response = txn.commit()
        print(commit_response)
    finally:
        txn.discard()


def search_person(client, name):
    query = """query search_person($a: int) {
        all(func: eq(age, $a)) {
            uid
            age
            gender
            reason
        }
    }"""

    variables = {'$a': name}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Number of people named {name}: {len(ppl['all'])}")
    print(f"Data associated with {name}:\n{json.dumps(ppl, indent=2)}")

##Recursive
def recursive(client, name):
    query = """query search_person($a: string) {
        all(func: eq(name, $a)) {
            uid
            name
            age
            loc
            dob
            ~follow {
                name
                age
        }
        }
    }"""

    variables = {'$a': name}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Number of people named {name}: {len(ppl['all'])}")
    print(f"Data associated with {name}:\n{json.dumps(ppl, indent=2)}")


def buscarAirline(client, name):
    query = """query search_airline($a: string) {
        all(func: has(age)) {
            flight @filter(eq(airline, $a) AND (eq(month,"1") OR eq(month,"12"))){
                count(airline)
                airline
                origin
                destination
                month
                }
        }
    }"""

    variables = {'$a': name}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Number of people in the airline {name}: {len(ppl['all'])}")
    print(f"Data associated with {name}:\n{json.dumps(ppl, indent=2)}")
##Buscar
def buscar(client, name):
    query = """query search_person($a: int) {
        all(func: eq(age, $a)) {
            uid
            name
            age
            loc
            dob
            follow {
                name
                age
        }
        }
    }"""

    variables = {'$a': name}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Number of people in the airline {name}: {len(ppl['all'])}")
    print(f"Data associated with {name}:\n{json.dumps(ppl, indent=2)}")

def buscarTodos(client, first_month, last_month):
    query = """query search_by_month($a: int, $b: int){
        all(func: has(age)) {
            flight @filter(eq(month, $a) OR eq(month, $b)) {
                count(airline)
                airline
                origin
                destination
                month
                }
            }
        }"""
    variables = {'$a': first_month, '$b': last_month}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Data associated with:\n{json.dumps(ppl, indent=2)}")

def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))

# ##Funcion que usa dos nodos
# def function_two_nodes(client):
#     query = """{
#         users(func: has(name)) {
#             uid
#             name
#             age
#             }
#             follow(func: eq(name, "Justin")) {
#                 uid
#                 name
#                 age
#         }
#     }"""

#     res = client.txn(read_only=True).query(query)
#     ppl = json.loads(res.json)

#     # Print results.
#     print(f"Query with two nodes :\n{json.dumps(ppl, indent=2)}")

# #Funcion para paginacion y ordenamiento
# def function_pagination(client):
#     query = """{
#         users(func: has(name), orderasc: age, first: 2, offset: 0) {
#             uid
#             name
#             age
#             follow(orderasc: age) {
#                 uid
#                 name
#                 age
#         }
#         }
#     }"""

#     res = client.txn(read_only=True).query(query)
#     ppl = json.loads(res.json)

#     # Print results.
#     print(f"Query with two nodes :\n{json.dumps(ppl, indent=2)}")