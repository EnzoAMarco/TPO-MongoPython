import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv

# variables .env
load_dotenv()
# inicializo variables de entorno
mongo_host = os.getenv("MONGO_HOST")
mongo_port = os.getenv("MONGO_PORT")
mongo_dbname = os.getenv("MONGO_DBNAME")

mongo_user = os.getenv("MONGO_USER")
mongo_pass = os.getenv("MONGO_PASS")
mongo_auth_source = os.getenv("MONGO_AUTH_SOURCE", "admin")

# path
if mongo_user and mongo_pass:
    MONGO_URI = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/?authSource={mongo_auth_source}"
    print("Usando URI con autenticación.")
else:
    MONGO_URI = f"mongodb://{mongo_host}:{mongo_port}/"
    print("Usando URI sin autenticación.")


client = None
try:
    print(f"Intentando conectar a: {MONGO_URI}")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print("¡Conexión a MongoDB local exitosa!")

    # --- Seleccionar una de tus 4 bases de datos ---
    db_name_to_use = mongo_dbname if mongo_dbname else 'test'
    
    db = client[db_name_to_use]
    print(f"Usando la base de datos: {db.name}")
    # Colecciones de la db
    print(f"Colecciones en '{db.name}': {db.list_collection_names()}")

    # nombre de la coleccion
    collection = db['estudiantes']
    print(f"Usando la colección: {collection.name}")

    # Cuento los documentos
    print("\n--- Operaciones ---")
    count = collection.count_documents({})
    print(f"Total de documentos en la colección '{collection.name}': {count}")
    print("-------------------\n")

except ConnectionFailure as e:
    print(f"Error: No se pudo conectar a MongoDB en {MONGO_URI}.")
    print("Verifica la configuración en .env y que mongod esté corriendo.")
    print(f"Error original: {e}")
except Exception as e:
    print(f"Ocurrió un error inesperado: {e}")

finally:
    if client:
        client.close()
        print("Conexión a MongoDB cerrada.")