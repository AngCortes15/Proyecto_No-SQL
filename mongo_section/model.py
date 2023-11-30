from pymongo import MongoClient
 
mongo_client = MongoClient("localhost:27017")
db = mongo_client.iteso
collection = db.airports

months = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembe', 'Octubre', 'Noviembre', 'Diciembre']

def get_recomendations_all():
    collection.aggregate([{"$group": {"_id": {"month": "$month", "destination": "$destination"}, "total": { "$sum" : 1}}}, {"$match": {"total": {"$gt": 4}}}])
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
        if airports_months.get(airport) != None:
            airports_months[airport].append(month)
        else:
            airports_months[airport] = [month]
    for j in airports_months:
            for x in airports_months[j]:
                print(f'Para el aeropuerto {j} los meses recomendados son: {months[x]}')

def get_all_months_airports():
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
            if airports_months.get(airport) != None:
                airports_months[airport].append(month)
            else:
                airports_months[airport] = [month]
    for j in airports_months:
        print(f'Para el aeropuerto {j} los meses con pasajeros son:', end= " ")
        for x in airports_months[j]:
            print(f'{months[x - 1]} ', end= " ")
        print()

def visitors_through_year_in_airport():
    print("Aeropuertos disponibles: ")
    airports = collection.distinct("destination")
    print(airports)
    ap = input('Selecciona un aeropuerto: ')
    query = [
        {
            "$group": {"_id": {"month": "$month", "destination": "$destination"}, "total": { "$sum": 1 } }
        },
        {
            "$match": { "_id.destination": ap }
        }
    ]
    for i in collection.aggregate(query):
        month = i['_id']['month']
        airport = i['_id']['destination']
        total = i['total']
        print(f'El aeropuerto {ap} tuvo {total} visitantes en el mes de {months[month - 1]}')