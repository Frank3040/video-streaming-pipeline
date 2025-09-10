import json
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from pymongo.errors import CollectionInvalid

# Cargar variables desde .env
load_dotenv()

MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB')
RUTA_JSON = os.getenv('RUTA_JSON', 'content.json')  # ruta configurable

def crear_db_y_colecciones(db):

    colecciones_existentes = db.list_collection_names()

    # Crear colección de movies con validador
    if "movies" not in colecciones_existentes:
        try:
            db.create_collection(
                "movies",
                validator={
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["content_id", "title", "genre"],
                        "properties": {
                            "content_id": {"bsonType": "string"},
                            "title": {"bsonType": "string"},
                            "genre": {"bsonType": "array"},
                            "release_year": {"bsonType": "int"},
                            "rating": {"bsonType": ["double", "int"]},
                            "views_count": {"bsonType": "int"}
                        }
                    }
                }
            )
            print("Colección 'movies' creada con validador.")
        except CollectionInvalid:
            print("Colección 'movies' ya existe.")

    # Crear colección de series con validador
    if "series" not in colecciones_existentes:
        try:
            db.create_collection(
                "series",
                validator={
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["content_id", "title", "genre"],
                        "properties": {
                            "content_id": {"bsonType": "string"},
                            "title": {"bsonType": "string"},
                            "genre": {"bsonType": "array"},
                            "seasons": {"bsonType": "int"},
                            "rating": {"bsonType": ["double", "int"]},
                            "total_views": {"bsonType": "int"}
                        }
                    }
                }
            )
            print("Colección 'series' creada con validador.")
        except CollectionInvalid:
            print("Colección 'series' ya existe.")


def cargar_json_a_mongodb():
    # Leer archivo JSON
    with open(RUTA_JSON, 'r', encoding='utf-8') as archivo:
        datos = json.load(archivo)

    peliculas = datos.get('movies', [])
    series = datos.get('series', [])

    if not peliculas and not series:
        raise ValueError("El JSON no contiene películas ni series.")

    # Conexión a MongoDB
    cliente = MongoClient(MONGO_URI)
    db = cliente[MONGO_DB]

    # Crear DB y colecciones explícitamente
    crear_db_y_colecciones(db)

    # Colecciones
    movies_collection = db["movies"]
    series_collection = db["series"]

    # Insertar películas
    if peliculas:
        resultado = movies_collection.insert_many(peliculas)
        print(f"✅ Se insertaron {len(resultado.inserted_ids)} películas en MongoDB.")

        # Crear índices recomendados
        movies_collection.create_index("title")
        movies_collection.create_index("release_year")
        movies_collection.create_index([("genre", 1), ("rating", -1)])

    # Insertar series
    if series:
        resultado = series_collection.insert_many(series)
        print(f"✅ Se insertaron {len(resultado.inserted_ids)} series en MongoDB.")

        # Crear índices recomendados
        series_collection.create_index("title")
        series_collection.create_index("seasons")
        series_collection.create_index([("genre", 1), ("rating", -1)])
    
    return db

def ejecutar_agregaciones(db):
    movies_collection = db["movies"]
    series_collection = db["series"]

    print("\n📊 Ejecutando Aggregation Pipelines:\n")

    # 1) Promedio de rating y presupuesto por año (películas) donde el rating promedio mayor a 3
    pipeline1 = [
        {"$group": {
            "_id": "$release_year",
            "avg_rating": {"$avg": "$rating"},
            "avg_budget": {"$avg": "$production_budget"}
        }},
        {"$match": {"avg_rating": {"$gte": 3}}},
        {"$sort": {"avg_budget": -1}}
    ]
    result1 = list(movies_collection.aggregate(pipeline1))
    print("🎬 Promedio de rating y presupuesto por año (películas):")
    for r in result1:
        print(r)

    # 2) Total de vistas por genero ordenado de forma ascendente y limitado a 3 datos (peliculas)
    pipeline2 = [
        
        {"$unwind": "$genre"
        },
        {"$group": {
            "_id": "$genre",
            "total_views": {
                "$sum": "$views_count"}}
        },
        {
            "$sort": {
            "total_views": -1
            }
        },
        {
            "$limit": 3
        }
        ]
    
    result2 = list(movies_collection.aggregate(pipeline2))
    print("\n📺 Total de vistas por genero ordenado de forma ascendente y limitado a 3 datos (peliculas):")
    for r in result2:
        print(r)

    # 3) Películas más vistas por género (series)
    pipeline3 = [
        {
            "$group": {
            "_id": "$genre",
            "total_views": { "$sum": "$total_views" },
            "avg_budget": { "$avg": "$production_budget" }
            }
        },
        {
            "$match": {
            "total_views": { "$gte": 100000 },
            "avg_budget": { "$gte": 40000000 }
            }
        },
        {
            "$sort": {
            "total_views": -1
            }
        }
        ]
    
    
    result3 = list(series_collection.aggregate(pipeline3))
    print("\n📊 Visualizaciones totales y promedio de ellos por genero donde visualizaciones mayores a 100000 y promedio de presupuesto mayor a 40000000 :")
    for r in result3:
        print(r)
        
    pipeline4 = [
        {
            "$project": {
            "title": 1,
            "seasons": 1,
            "episodes_per_season": 1,
            "total_episodes": { "$sum": "$episodes_per_season" }
            }
        },
        {
            "$match": {
            "seasons": { "$gte": 5 },
            "total_episodes": { "$gte": 50 }
            }
        },
        {
            "$sort": {
            "total_episodes": -1
            }
        }]

    result4 = list(series_collection.aggregate(pipeline4))
    print("\n📊 identificar los contenidos que tienen un número significativo de temporadas (5 o más) y un número total de episodios alto (50 o más)")
    for r in result4:
        print(r)

if __name__ == '__main__':
    db = cargar_json_a_mongodb()
    
    ejecutar_agregaciones(db)