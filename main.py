import streamlit as st
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

# pag streamlit
st.set_page_config(page_title="Consultas MongoDB", layout="wide")

#datos DB
load_dotenv()

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_DBNAME = os.getenv("MONGO_DBNAME")
MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"
CONN_INFO = f"{MONGO_HOST}:{MONGO_PORT} (Sin autenticar)"

# cone a la DB
def init_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        print("Conexión a MongoDB establecida.")
        return client
    except pymongo.errors.ConnectionFailure as e:
        st.error(f"Error al conectar a MongoDB: {e}")
        return None
    except Exception as e:
        st.error(f"Error inesperado al conectar: {e}")
        return None

# inicializo la cone
client = init_connection()

# obtengo la db(libros) dentro de la cone
db = client[MONGO_DBNAME]

# --> CONSULTAS <--

def consulta_1_promedio_nominados(_db):
    
    nominaciones_coll = _db["nominaciones"]
    libros_coll = _db["libros"]

    cursor_nominaciones = nominaciones_coll.find({}, {"libro": 1, "_id": 0})
    book_ids = [nom.get('libro') for nom in cursor_nominaciones if nom.get('libro')]

    cursor_libros = libros_coll.find(
        {"_id": {"$in": book_ids}},
        {"anio_publicacion": 1, "_id": 0}
    )
    libros_nominados = list(cursor_libros)

    df_libros = pd.DataFrame(libros_nominados)
    df_libros['anio_publicacion'] = pd.to_numeric(df_libros['anio_publicacion'], errors='coerce')
    promedio = df_libros['anio_publicacion'].mean(skipna=True)

    return pd.DataFrame({'promedio_anio': [promedio]})

# encuentra el género que más premios ganó.
def consulta_2_genero_mas_ganador(_db):

    nominaciones_coll = _db["nominaciones"]
    libros_coll = _db["libros"]
    generos_coll = _db["generos"]

    cursor_ganadores = nominaciones_coll.find({"ganador": True}, {"libro": 1, "_id": 0})
    book_ids_ganadores = [nom.get('libro') for nom in cursor_ganadores if nom.get('libro')]

    cursor_libros = libros_coll.find(
        {"_id": {"$in": book_ids_ganadores}},
        {"genero": 1, "_id": 0}
    )
    generos_ids_libros_ganadores = [libro.get('genero') for libro in cursor_libros if libro.get('genero')]

    df_genero_ids = pd.DataFrame({'genero_id': generos_ids_libros_ganadores})
    df_counts = df_genero_ids['genero_id'].value_counts().reset_index()
    df_counts.columns = ['_id', 'total_premios']

    cursor_generos = generos_coll.find({}, {"nombre": 1, "_id": 1})
    df_generos = pd.DataFrame(list(cursor_generos))
    
    df_merged = pd.merge(df_counts, df_generos, on='_id', how='inner')

    df_result = df_merged.sort_values(by='total_premios', ascending=False).head(1)
    df_final = df_result[['nombre', 'total_premios']].rename(columns={'nombre': 'nombre_genero'})

    return df_final

# encuentra el idioma que más premios ganó.
def consulta_3_idioma_mas_ganador(_db):

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

# encuentra el idioma con más nominaciones.
def consulta_4_idioma_mas_nominado(_db):

    nominaciones_coll = _db["nominaciones"]
    libros_coll = _db["libros"]
    idiomas_coll = _db["idiomas"]

    cursor_nominaciones = nominaciones_coll.find({}, {"libro": 1, "_id": 0})
    book_ids_nominados = [nom.get('libro') for nom in cursor_nominaciones if nom.get('libro')]
    
    cursor_libros = libros_coll.find({"_id": {"$in": book_ids_nominados}}, {"idioma": 1, "_id": 0})
    idiomas_ids_libros_nominados = [libro.get('idioma') for libro in cursor_libros if libro.get('idioma')]
    
    df_idioma_ids = pd.DataFrame({'idioma_id': idiomas_ids_libros_nominados})
    df_counts = df_idioma_ids['idioma_id'].value_counts().reset_index()
    df_counts.columns = ['_id', 'total_nominaciones']

    cursor_idiomas = idiomas_coll.find({}, {"nombre": 1, "_id": 1})
    df_idiomas = pd.DataFrame(list(cursor_idiomas))
    
    df_merged = pd.merge(df_counts, df_idiomas, on='_id', how='inner')
    df_result = df_merged.sort_values(by='total_nominaciones', ascending=False).head(1)
    df_final = df_result[['nombre', 'total_nominaciones']].rename(columns={'nombre': 'nombre_idioma'})

    return df_final

# calcula promedio de año de libros de Distopía en Español.
def consulta_5_promedio_cf_es(_db):
    
    generos_coll = _db["generos"]
    idiomas_coll = _db["idiomas"]
    libros_coll = _db["libros"]

    genero_cf = generos_coll.find_one({"nombre": "Distopía"}, {"_id": 1})
    cf_id = genero_cf['_id']
    idioma_es = idiomas_coll.find_one({"nombre": "Español"}, {"_id": 1})
    
    es_id = idioma_es['_id']
    cursor_libros = libros_coll.find(
        {"genero": cf_id, "idioma": es_id},
        {"anio_publicacion": 1, "_id": 0}
    )
    libros_cf_es = list(cursor_libros)
    
    df_libros = pd.DataFrame(libros_cf_es)
    df_libros['anio_publicacion'] = pd.to_numeric(df_libros['anio_publicacion'], errors='coerce')
    promedio = df_libros['anio_publicacion'].mean(skipna=True)

    return pd.DataFrame({'promedio_anio': [promedio]})

# cuenta libros del género misterio.
def consulta_6_contar_misterio(_db):


    generos_coll = _db["generos"]
    libros_coll = _db["libros"]
    genero_pol = generos_coll.find_one({"nombre": "Misterio"}, {"_id": 1})
    misterio_id = genero_pol['_id']
    count = libros_coll.count_documents({"genero": misterio_id})

    return pd.DataFrame({'total_libros_misterio': [count]})

#Cuenta libros de Distopia en Español.
def consulta_7_contar_Distopia_es(_db):

    generos_coll = _db["generos"]
    idiomas_coll = _db["idiomas"]
    libros_coll = _db["libros"]
    genero_ter = generos_coll.find_one({"nombre": "Distopía"}, {"_id": 1})
    Distopia_id = genero_ter['_id']
    idioma_es = idiomas_coll.find_one({"nombre": "Español"}, {"_id": 1})
    es_id = idioma_es['_id']
    count = libros_coll.count_documents({"genero": Distopia_id, "idioma": es_id})

    return pd.DataFrame({'total_libros_Distopia_espanol': [count]})


# UI de streamlit
st.title("📊 Consultas a MongoDB con Pandas & Streamlit")

if db is None:
    st.error("No se pudo conectar a la base de datos.")
    st.stop()

st.sidebar.info(f"Conectado a: {CONN_INFO}")
st.sidebar.caption(f"Base de datos: {db.name}")

# crear pestañas
tab_titles = [
    "1: Promedio Año (Nominados)",
    "2: Género + Ganador",
    "3: Idioma + Ganador",
    "4: Idioma + Nominado",
    "5: Promedio Año (Distopía, Español)",
    "6: Conteo Libros de Misterio",
    "7: Conteo Libros (Distopia, Español)",
    "8: Ver todas las collections"
]
tabs = st.tabs(tab_titles)

# contenido de pestañas/tabs

with tabs[0]:
    
    st.subheader(tab_titles[0])
    df_q1 = consulta_1_promedio_nominados(db)
    st.dataframe(df_q1, use_container_width=True)
    
    if not df_q1.empty and 'promedio_anio' in df_q1.columns:
        
        try:
            avg_year = df_q1['promedio_anio'].iloc[0]
            st.metric(label="Promedio Año Publicación", value=f"{avg_year:.2f}" if pd.notna(avg_year) else "N/A")
        except (KeyError, IndexError, TypeError):
            st.warning("No se pudo extraer el valor del promedio.")
            
    else:
         st.info("La consulta no devolvió resultados o no se pudo calcular.")

with tabs[1]:
    
    st.subheader(tab_titles[1])
    df_q2 = consulta_2_genero_mas_ganador(db)
    st.dataframe(df_q2, use_container_width=True)
    
    if not df_q2.empty and 'nombre_genero' in df_q2.columns and 'total_premios' in df_q2.columns:
        
        try:
            top_genre = df_q2['nombre_genero'].iloc[0]
            top_genre_awards = df_q2['total_premios'].iloc[0]
            st.metric(label="Género Más Ganador", value=top_genre, delta=f"{top_genre_awards} premios", delta_color="off")
        except (KeyError, IndexError):
            st.warning("No se pudieron extraer los valores del resultado.")
            
    elif not df_q2.empty:
        st.warning("Las columnas esperadas ('nombre_genero', 'total_premios') no se encontraron.")
    else:
        st.info("La consulta no devolvió resultados.")

with tabs[2]:
    
    st.subheader(tab_titles[2])
    df_q3 = consulta_3_idioma_mas_ganador(db)
    st.dataframe(df_q3, use_container_width=True)
    
    if not df_q3.empty and 'nombre_idioma' in df_q3.columns and 'total_premios' in df_q3.columns:
        
        try:
            top_lang = df_q3['nombre_idioma'].iloc[0]
            top_lang_awards = df_q3['total_premios'].iloc[0]
            st.metric(label="Idioma Más Ganador", value=top_lang, delta=f"{top_lang_awards} premios", delta_color="off")
        except (KeyError, IndexError):
            st.warning("No se pudieron extraer los valores del resultado.")
            
    elif not df_q3.empty:
         st.warning("Las columnas esperadas ('nombre_idioma', 'total_premios') no se encontraron.")
    else:
        st.info("La consulta no devolvió resultados.")

with tabs[3]:
    
    st.subheader(tab_titles[3])
    df_q4 = consulta_4_idioma_mas_nominado(db)
    st.dataframe(df_q4, use_container_width=True)
    
    if not df_q4.empty and 'nombre_idioma' in df_q4.columns and 'total_nominaciones' in df_q4.columns:
        
        try:
            top_lang_nom = df_q4['nombre_idioma'].iloc[0]
            top_lang_nom_count = df_q4['total_nominaciones'].iloc[0]
            st.metric(label="Idioma Más Nominado", value=top_lang_nom, delta=f"{top_lang_nom_count} nominaciones", delta_color="off")
        except (KeyError, IndexError):
            st.warning("No se pudieron extraer los valores del resultado.")
            
    elif not df_q4.empty:
         st.warning("Las columnas esperadas ('nombre_idioma', 'total_nominaciones') no se encontraron.")
    else:
        st.info("La consulta no devolvió resultados.")

with tabs[4]:
    
    st.subheader(tab_titles[4])
    df_q5 = consulta_5_promedio_cf_es(db)
    st.dataframe(df_q5, use_container_width=True)
    
    if not df_q5.empty and 'promedio_anio' in df_q5.columns:
        
        try:
            avg_year_cf_es = df_q5['promedio_anio'].iloc[0]
            st.metric(label="Promedio Año (Distopía, Español)", value=f"{avg_year_cf_es:.2f}" if pd.notna(avg_year_cf_es) else "N/A")
        except (KeyError, IndexError, TypeError):
            st.warning("No se pudo extraer el valor del promedio.")
            
    elif not df_q5.empty:
         st.warning("La columna 'promedio_anio' no se encontró en los resultados.")
    else:
         st.info("La consulta no devolvió resultados (¿Existen libros de Distopía en Español con año?).")

with tabs[5]:
    
    st.subheader(tab_titles[5])
    df_q6 = consulta_6_contar_misterio(db)
    st.dataframe(df_q6, use_container_width=True)
    
    if not df_q6.empty and 'total_libros_misterio' in df_q6.columns:
        try:
            count_misterio = df_q6['total_libros_misterio'].iloc[0]
            st.metric(label="Total Libros Misterios", value=int(count_misterio))
        except (KeyError, IndexError, TypeError):
            st.warning("No se pudo extraer el valor del conteo.")

with tabs[6]:
    
    st.subheader(tab_titles[6])
    st.button("boton")
    df_q7 = consulta_7_contar_Distopia_es(db)
    st.dataframe(df_q7, use_container_width=True)
    
    if not df_q7.empty and 'total_libros_Distopia_espanol' in df_q7.columns:
        
        try:
            count_Distopia_es = df_q7['total_libros_Distopia_espanol'].iloc[0]
            st.metric(label="Total Libros (Distopia, Español)", value=int(count_Distopia_es))
        except (KeyError, IndexError, TypeError):
             st.warning("No se pudo extraer el valor del conteo.")

with tabs[7]:
    
    st.subheader("Colección Libros")
    df_libros = list(db["libros"].find().limit(5))
    st.dataframe(df_libros, use_container_width=True)
    st.divider()
    
    st.subheader("Colección Autores")
    df_autores = list(db["autores"].find().limit(5))
    st.dataframe(df_autores, use_container_width=True)
    st.divider()

    st.subheader("Colección Formatos")
    df_formatos = list(db["formatos"].find().limit(5))
    st.dataframe(df_formatos, use_container_width=True)
    st.divider()

    st.subheader("Colección Géneros")
    df_generos = list(db["generos"].find().limit(5))
    st.dataframe(df_generos, use_container_width=True)
    st.divider()

    st.subheader("Colección Idiomas")
    df_idiomas = list(db["idiomas"].find().limit(5))
    st.dataframe(df_idiomas, use_container_width=True)
    st.divider()

    st.subheader("Colección Nominaciones")
    df_nominaciones = list(db["nominaciones"].find().limit(5))
    st.dataframe(df_nominaciones, use_container_width=True)
    st.divider()

    st.subheader("Colección Premios")
    df_premios = list(db["premios"].find().limit(5))
    st.dataframe(df_premios, use_container_width=True)