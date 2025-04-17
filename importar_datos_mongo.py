import json
from pymongo import MongoClient

# cone a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['LibrosTesteo']

# guardamos el json en la var data
with open('Libros_Grupo8.json') as file:
    data = json.load(file)

# itera en el .json para crear las collections con sus documents
for collection_name, documents in data.items():
    collection = db[collection_name]
    collection.insert_many(documents)

print("Datos importados correctamente.")
