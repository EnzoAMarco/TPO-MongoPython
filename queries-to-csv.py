import pymongo
from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv
import sys

# recupero los datos entorno/.env
load_dotenv()
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_DBNAME = os.getenv("MONGO_DBNAME")
MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"

# metodo para establecer la cone
def init_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        print("Conexión a MongoDB establecida.")
        return client
    except pymongo.errors.ConnectionFailure as e:
        print(f"Error al conectar a MongoDB: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error inesperado al conectar: {e}", file=sys.stderr)
        return None

client = init_connection()
db = client[MONGO_DBNAME]

# querie que emplea pandas
def consulta_3_idioma_mas_ganador(_db):

    if _db is None: return pd.DataFrame()
    
    nominaciones_coll = _db["nominaciones"]
    libros_coll = _db["libros"]
    idiomas_coll = _db["idiomas"]

    cursor_ganadores = nominaciones_coll.find({"ganador": True}, {"libro": 1, "_id": 0})
    book_ids_ganadores = [nom.get('libro') for nom in cursor_ganadores if nom.get('libro')]

    cursor_libros = libros_coll.find({"_id": {"$in": book_ids_ganadores}}, {"idioma": 1, "_id": 0}) # Asume 'idioma' es ID
    idiomas_ids_libros_ganadores = [libro.get('idioma') for libro in cursor_libros if libro.get('idioma')]

    df_idioma_ids = pd.DataFrame({'idioma_id': idiomas_ids_libros_ganadores})
    df_counts = df_idioma_ids['idioma_id'].value_counts().reset_index()
    df_counts.columns = ['_id', 'total_premios']

    cursor_idiomas = idiomas_coll.find({}, {"nombre": 1, "_id": 1})
    df_idiomas = pd.DataFrame(list(cursor_idiomas))

    df_merged = pd.merge(df_counts, df_idiomas, on='_id', how='inner')
    df_result = df_merged.sort_values(by='total_premios', ascending=False).head(1)
    df_final = df_result[['nombre', 'total_premios']].rename(columns={'nombre': 'nombre_idioma'})

    return df_final

# variable guardando el DF
df_para_guardar = consulta_3_idioma_mas_ganador(db)

# transformo el DF en un .CSV
df_para_guardar.to_csv("queries-consulta-2.csv")
print(f"Resultados guardados exitosamente en queries-consulta-2.csv")
