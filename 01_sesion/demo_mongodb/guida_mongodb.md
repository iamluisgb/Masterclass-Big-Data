# Guía de MongoDB con Python

Esta guía proporciona una introducción práctica al uso de MongoDB con Python. Aprenderás a configurar un entorno con Docker, realizar operaciones CRUD, ejecutar consultas avanzadas y utilizar el framework de agregación de MongoDB para analizar datos.

## Tabla de Contenidos

1. [Introducción](#introducción)
2. [Configuración del Entorno](#configuración-del-entorno)
3. [Conexión a MongoDB desde Python](#conexión-a-mongodb-desde-python)
4. [Operaciones CRUD Básicas](#operaciones-crud-básicas)
5. [Consultas Avanzadas](#consultas-avanzadas)
6. [Framework de Agregación](#framework-de-agregación)
7. [Buenas Prácticas](#buenas-prácticas)
8. [Ejercicio Práctico](#ejercicio-práctico)
9. [Demo Completa: Análisis de Ventas](#demo-completa-análisis-de-ventas)

## Introducción

MongoDB es una base de datos NoSQL orientada a documentos que ofrece un modelo de datos flexible basado en documentos JSON/BSON. Python, por su parte, cuenta con un amplio ecosistema de bibliotecas para análisis de datos. La combinación de MongoDB y Python proporciona una solución potente para trabajar con datos no estructurados o semiestructurados.

**Ventajas de MongoDB**:
- Modelo de datos flexible basado en documentos JSON/BSON
- Esquema dinámico que no requiere definición previa
- Escalabilidad horizontal nativa
- Consultas ricas y expresivas sobre estructuras complejas
- Características avanzadas como índices geoespaciales y búsqueda de texto completo

**Ventajas de Python para análisis de datos**:
- Sintaxis clara y legible
- Ecosistema de bibliotecas especializadas (pandas, NumPy, matplotlib)
- Facilidad para manipulación y visualización de datos
- Integración con diversas tecnologías y sistemas

## Configuración del Entorno

### Instalación de MongoDB con Docker

Utilizaremos Docker para crear nuestro entorno de MongoDB de forma rápida y aislada:

```bash
# Descargar la imagen oficial de MongoDB
docker pull mongodb/mongodb-community-server

# Crear y ejecutar un contenedor de MongoDB
docker run --name mongodb-curso \
  -e MONGO_INITDB_ROOT_USERNAME=usuario \
  -e MONGO_INITDB_ROOT_PASSWORD=micontraseña \
  -p 27017:27017 \
  -d mongodb/mongodb-community-server
```

Para verificar que el contenedor está en funcionamiento:

```bash
docker ps
```

### Instalación de bibliotecas Python

Necesitaremos instalar las bibliotecas Python para trabajar con MongoDB:

```bash
# Instalar el driver oficial de MongoDB para Python
pip install pymongo

# Opcionalmente, instalar MongoEngine (ODM para MongoDB)
pip install mongoengine
```

## Conexión a MongoDB desde Python

### PyMongo: Driver Oficial

PyMongo proporciona una API que mapea directamente a los comandos y conceptos de MongoDB:

```python
import pymongo

# Establecer conexión
client = pymongo.MongoClient("mongodb://usuario:micontraseña@localhost:27017/")
db = client["analisis_datos"]
coleccion = db["productos"]

# Verificar la conexión
try:
    client.admin.command('ping')
    print("Conexión establecida correctamente!")
except Exception as e:
    print(f"Error al conectar: {e}")

# Importante: cerrar la conexión al finalizar
client.close()
```

### MongoEngine: ODM para MongoDB

MongoEngine permite definir esquemas estructurados para documentos MongoDB:

```python
from mongoengine import Document, StringField, FloatField, IntField, connect

# Conectar a la base de datos
connect('analisis_datos', host='mongodb://usuario:micontraseña@localhost:27017/analisis_datos?authSource=admin')

# Definir una clase para documentos
class Producto(Document):
    nombre = StringField(required=True, max_length=200)
    categoria = StringField(required=True)
    precio = FloatField(required=True)
    stock = IntField(default=0)

    meta = {'collection': 'productos'}
```

### Uso de Context Managers

Para gestionar conexiones de forma segura:

```python
from pymongo import MongoClient
from contextlib import contextmanager

@contextmanager
def get_mongo_client(connection_string):
    client = MongoClient(connection_string)
    try:
        yield client
    finally:
        print('Cerrando conexión...')
        client.close()

# Uso del context manager
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    productos = db.productos.find()
    for producto in productos:
        print(f"{producto['nombre']}: ${producto['precio']}")
```

## Operaciones CRUD Básicas

### Crear (Create)

```python
# Insertar un documento
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    resultado = db.productos.insert_one({
        "nombre": "Monitor UltraHD",
        "categoria": "Electrónicos",
        "precio": 349.99,
        "stock": 8,
        "especificaciones": {
            "tamaño": "27 pulgadas",
            "resolución": "3840x2160",
            "tipo": "IPS"
        }
    })
    print(f"ID del documento insertado: {resultado.inserted_id}")

# Insertar múltiples documentos
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    nuevos_clientes = [
        {
            "nombre": "David López",
            "email": "david@ejemplo.com",
            "edad": 31,
            "intereses": ["tecnología", "música", "fotografía"]
        },
        {
            "nombre": "Laura Martínez",
            "email": "laura@ejemplo.com",
            "edad": 27,
            "intereses": ["viajes", "gastronomía"]
        }
    ]
    resultado = db.clientes.insert_many(nuevos_clientes)
    print(f"IDs de documentos insertados: {resultado.inserted_ids}")
```

### Leer (Read)

```python
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos

    # Encontrar un documento específico
    producto = db.productos.find_one({"nombre": "Monitor UltraHD"})
    if producto:
        print(f"Producto encontrado: {producto['nombre']} - ${producto['precio']}")

    # Encontrar múltiples documentos con filtros
    query = {"categoria": "Electrónicos", "precio": {"$lt": 500}}
    productos = db.productos.find(query).sort("precio", -1)
    print("Productos electrónicos por debajo de $500:")
    for producto in productos:
        print(f"- {producto['nombre']}: ${producto['precio']}")

    # Proyección: seleccionar solo ciertos campos
    productos = db.productos.find(
        {"stock": {"$gt": 0}},
        {"nombre": 1, "precio": 1, "stock": 1, "_id": 0}
    )
    print("\nProductos en stock (solo nombre, precio y cantidad):")
    for producto in productos:
        print(f"- {producto['nombre']}: ${producto['precio']} ({producto['stock']} unidades)")

    # Consultas avanzadas con operadores
    clientes_tech = db.clientes.find({"intereses": {"$in": ["tecnología"]}})
    print("\nClientes interesados en tecnología:")
    for cliente in clientes_tech:
        print(f"- {cliente['nombre']} ({cliente['email']})")
```

### Actualizar (Update)

```python
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    # Actualizar un solo documento
    resultado = db.productos.update_one(
        {"nombre": "Monitor UltraHD"},
        {"$set": {"precio": 329.99, "promocion": True}}
    )
    print(f"Documentos modificados: {resultado.modified_count}")

    # Actualizar múltiples documentos
    resultado = db.productos.update_many(
        {"categoria": "Electrónicos", "promocion": {"$exists": False}},
        {"$set": {"promocion": False}}
    )
    print(f"Electrónicos actualizados: {resultado.modified_count}")

    # Operadores de actualización
    # $inc para incrementar valores
    db.productos.update_one(
        {"nombre": "Monitor UltraHD"},
        {"$inc": {"stock": -1}}  # Reducir stock en 1
    )

    # $push para añadir a arrays
    db.clientes.update_one(
        {"email": "laura@ejemplo.com"},
        {"$push": {"intereses": "deportes"}}
    )

    # Upsert (insertar si no existe)
    db.productos.update_one(
        {"sku": "MON-UHD-27"},  # Documento que quizás no existe
        {"$set": {"nombre": "Monitor UltraHD", "sku": "MON-UHD-27"}},
        upsert=True
    )
```

### Eliminar (Delete)

```python
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    # Eliminar un solo documento
    resultado = db.clientes.delete_one({"email": "laura@ejemplo.com"})
    print(f"Documentos eliminados: {resultado.deleted_count}")

    # Eliminar múltiples documentos
    resultado = db.productos.delete_many({"stock": 0, "descontinuado": True})
    print(f"Productos obsoletos eliminados: {resultado.deleted_count}")

    # Eliminar colección
    db.temp_collection.drop()
```

## Consultas Avanzadas

MongoDB ofrece capacidades de consulta avanzadas que podemos aprovechar desde Python:

### Consultas con Expresiones Regulares

```python
import re

with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    regex_pattern = re.compile('ultra', re.IGNORECASE)
    productos = db.productos.find({"nombre": regex_pattern})
    
    for producto in productos:
        print(f"- {producto['nombre']} (${producto['precio']})")
```

### Consultas Geoespaciales

```python
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    # Asegurarse de que existe un índice geoespacial
    db.tiendas.create_index([("ubicacion", "2dsphere")])
    
    # Buscar tiendas cercanas a una ubicación (Madrid)
    tiendas_cercanas = db.tiendas.find({
        "ubicacion": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [-3.7037902, 40.4167754]
                },
                "$maxDistance": 300_000  # 300 km
            }
        }
    })
    
    for tienda in tiendas_cercanas:
        print(f"- {tienda['nombre']} ({tienda['direccion']})")
```

### Búsqueda de Texto Completo

```python
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    # Crear índice de texto (si no existe)
    db.productos.create_index([("descripcion", "text")])
    
    # Buscar productos por palabras clave en la descripción
    productos = db.productos.find({"$text": {"$search": "alta calidad"}})
    
    for producto in productos:
        print(f"- {producto['nombre']}: {producto['descripcion']}")
```

## Framework de Agregación

El Framework de Agregación de MongoDB es una de sus características más poderosas para el análisis de datos. A diferencia de las consultas simples, que sólo filtran y recuperan documentos, el framework de agregación permite transformar, combinar y analizar datos dentro de la base de datos antes de retornarlos a la aplicación.

### Conceptos Fundamentales

El Framework de Agregación de MongoDB se basa en el concepto de **pipeline** (tubería), donde los documentos fluyen a través de diferentes etapas de transformación. Cada etapa toma los documentos de la etapa anterior, los procesa y pasa los resultados a la siguiente etapa.

### Etapas Principales

1. **`$match`**: Filtra documentos (similar a `find()`)
2. **`$group`**: Agrupa documentos y calcula valores por grupo
3. **`$project`**: Selecciona y transforma campos específicos
4. **`$sort`**: Ordena los documentos
5. **`$limit`/`$skip`**: Limita o salta un número de documentos
6. **`$unwind`**: Descompone arrays en documentos individuales
7. **`$lookup`**: Realiza un "join" con otra colección
8. **`$addFields`**: Añade nuevos campos calculados
9. **`$facet`**: Permite procesamiento paralelo en un mismo conjunto de datos

### Operadores de Agregación

Dentro de las etapas se pueden utilizar diversos operadores:

- **Acumuladores**: `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, etc.
- **Aritméticos**: `$add`, `$subtract`, `$multiply`, `$divide`, etc.
- **Fechas**: `$year`, `$month`, `$dayOfMonth`, `$hour`, etc.
- **Condicionales**: `$cond`, `$switch`, `$ifNull`, etc.
- **Arrays**: `$size`, `$filter`, `$map`, `$reduce`, etc.

### Análisis Básico: Ventas por Categoría

```python
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    pipeline = [
        # Etapa 1: Filtrar solo ventas completadas
        {"$match": {"estado": "completado"}},

        # Etapa 2: Agrupar por categoría
        {"$group": {
            "_id": "$categoria_producto",
            "total_ventas": {"$sum": "$monto"},
            "cantidad_ventas": {"$sum": 1},
            "promedio_venta": {"$avg": "$monto"}
        }},

        # Etapa 3: Ordenar por total de ventas
        {"$sort": {"total_ventas": -1}}
    ]

    resultados = db.ventas.aggregate(pipeline)
    
    for resultado in resultados:
        print(f"Categoría: {resultado['_id']}")
        print(f"  Total ventas: ${resultado['total_ventas']:,.2f}")
        print(f"  Cantidad: {resultado['cantidad_ventas']}")
        print(f"  Promedio: ${resultado['promedio_venta']:,.2f}")
```

### Análisis Avanzado: Ventas por Región y Tiempo

```python
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    pipeline = [
        # Etapa 1: Añadir campos calculados para fecha
        {"$addFields": {
            "mes": {"$month": "$fecha_venta"},
            "año": {"$year": "$fecha_venta"}
        }},

        # Etapa 2: Agrupar por región, año y mes
        {"$group": {
            "_id": {
                "region": "$region_cliente",
                "año": "$año",
                "mes": "$mes"
            },
            "total_ventas": {"$sum": "$monto"},
            "cantidad_transacciones": {"$sum": 1},
            "productos_vendidos": {"$sum": "$cantidad_productos"}
        }},

        # Etapa 3: Agrupar de nuevo para obtener totales anuales por región
        {"$group": {
            "_id": {
                "region": "$_id.region",
                "año": "$_id.año"
            },
            "ventas_mensuales": {"$push": {
                "mes": "$_id.mes",
                "ventas": "$total_ventas",
                "transacciones": "$cantidad_transacciones"
            }},
            "total_anual": {"$sum": "$total_ventas"},
            "promedio_mensual": {"$avg": "$total_ventas"}
        }},
        
        # Etapa 4: Ordenar por región y año
        {"$sort": {"_id.region": 1, "_id.año": 1}}
    ]

    resultados = db.ventas.aggregate(pipeline)

    for resultado in resultados:
        region = resultado["_id"]["region"]
        año = resultado["_id"]["año"]
        print(f"Región: {region}, Año: {año}")
        print(f"  Total anual: ${resultado['total_anual']:,.2f}")
        print(f"  Promedio mensual: ${resultado['promedio_mensual']:,.2f}")
        print("  Desglose mensual:")
        for mes in sorted(resultado["ventas_mensuales"], key=lambda x: x["mes"]):
            print(f"    Mes {mes['mes']}: ${mes['ventas']:,.2f} ({mes['transacciones']} transacciones)")
        print()
```

### Análisis de Productos por Valoración

```python
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    pipeline = [
        # Etapa 1: Agrupar por categoría
        {"$group": {
            "_id": "$categoria",
            "num_productos": {"$sum": 1},
            "valoracion_media": {"$avg": "$valoracion"},
            "precio_medio": {"$avg": "$precio"},
            "stock_total": {"$sum": "$stock"}
        }},
        
        # Etapa 2: Ordenar por valoración media (descendente)
        {"$sort": {"valoracion_media": -1}}
    ]

    resultados = db.productos.aggregate(pipeline)

    print("\nCategorías de productos por valoración media:")
    print("=============================================")
    for resultado in resultados:
        print(f"Categoría: {resultado['_id']}")
        print(f"  Valoración media: {resultado['valoracion_media']:.1f}/5.0")
        print(f"  Número de productos: {resultado['num_productos']}")
        print(f"  Precio medio: ${resultado['precio_medio']:.2f}")
        print(f"  Stock total: {resultado['stock_total']} unidades")
        print()
```

## Buenas Prácticas

### Gestión Segura de Credenciales

Nunca codifiques las credenciales directamente en el código. Utiliza:

1. **Variables de entorno**:
   ```python
   import os
   from pymongo import MongoClient

   client = MongoClient(
       host=os.environ.get('MONGO_HOST', 'localhost'),
       port=int(os.environ.get('MONGO_PORT', 27017)),
       username=os.environ.get('MONGO_USER'),
       password=os.environ.get('MONGO_PASSWORD'),
       authSource=os.environ.get('MONGO_AUTH_DB', 'admin')
   )
   ```

2. **Archivos de configuración**:
   ```python
   import configparser
   from pymongo import MongoClient

   config = configparser.ConfigParser()
   config.read('config.ini')

   mongo_section = config['mongodb']
   client = MongoClient(
       host=mongo_section.get('host', 'localhost'),
       port=mongo_section.getint('port', 27017),
       username=mongo_section.get('username'),
       password=mongo_section.get('password'),
       authSource=mongo_section.get('authSource', 'admin')
   )
   ```

### Uso de Context Managers

Utiliza context managers para garantizar que las conexiones se cierren correctamente:

```python
from contextlib import contextmanager
from pymongo import MongoClient

@contextmanager
def get_mongo_client(connection_string):
    client = MongoClient(connection_string)
    try:
        yield client
    finally:
        print('Cerrando conexión...')
        client.close()
```

### Indexación Adecuada

Crea índices para mejorar el rendimiento de las consultas:

```python
# Índice simple para campos frecuentemente consultados
db.productos.create_index([("categoria", 1)])

# Índice compuesto para consultas que filtran por múltiples campos
db.productos.create_index([("categoria", 1), ("precio", -1)])

# Índice de texto para búsqueda de texto completo
db.productos.create_index([("descripcion", "text")])

# Índice geoespacial para consultas de ubicación
db.tiendas.create_index([("ubicacion", "2dsphere")])
```

## Demo Básica


Crea una colección "libros" y realiza las siguientes operaciones:

1. Inserta al menos 5 documentos de libros con campos como: título, autor, año de publicación, género, precio, editorial y ciudad
2. Busca todos los libros publicados en una ciudad específica
3. Actualiza el precio de un libro
4. Elimina un libro por su título
5. Muestra todos los libros ordenados por año de publicación
6. Encuentra el libro más antiguo
7. Busca libros de una editorial específica con precio menor a cierto valor

### Solución

```python
from pymongo import MongoClient
import pprint
from contextlib import contextmanager

@contextmanager
def get_mongo_client(connection_string):
    client = MongoClient(connection_string)
    try:
        yield client
    finally:
        print('Cerrando conexión...')
        client.close()

with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    coleccion = db['libros']
    
    # Limpiar colección para empezar de cero
    coleccion.drop()
    print("Colección 'libros' limpiada para comenzar de nuevo")
    
    # 1. CREAR: Insertar libros
    libros = [
        {
            "titulo": "La sombra del viento",
            "autor": "Carlos Ruiz Zafón",
            "año": 2001,
            "genero": "Novela histórica",
            "precio": 22.50,
            "editorial": "Planeta",
            "ciudad": "Barcelona"
        },
        {
            "titulo": "Patria",
            "autor": "Fernando Aramburu",
            "año": 2016,
            "genero": "Novela social",
            "precio": 21.90,
            "editorial": "Tusquets",
            "ciudad": "Barcelona"
        },
        {
            "titulo": "Nada",
            "autor": "Carmen Laforet",
            "año": 1944,
            "genero": "Novela",
            "precio": 18.50,
            "editorial": "Destino",
            "ciudad": "Madrid"
        },
        {
            "titulo": "Fortunata y Jacinta",
            "autor": "Benito Pérez Galdós",
            "año": 1887,
            "genero": "Novela realista",
            "precio": 24.00,
            "editorial": "Cátedra",
            "ciudad": "Madrid"
        },
        {
            "titulo": "Luces de bohemia",
            "autor": "Ramón María del Valle-Inclán",
            "año": 1924,
            "genero": "Teatro",
            "precio": 15.95,
            "editorial": "Espasa",
            "ciudad": "Madrid"
        }
    ]
    
    resultado = coleccion.insert_many(libros)
    print(f"Se insertaron {len(resultado.inserted_ids)} libros")
    
    # 2. LEER: Buscar libros por ciudad de publicación
    ciudad_buscar = "Barcelona"
    print(f"\nLibros publicados en '{ciudad_buscar}':")
    for libro in coleccion.find({"ciudad": ciudad_buscar}):
        pprint.pprint(libro)
    
    # 3. ACTUALIZAR: Modificar el precio de un libro
    titulo_actualizar = "Patria"
    nuevo_precio = 23.50
    
    resultado = coleccion.update_one(
        {"titulo": titulo_actualizar},
        {"$set": {"precio": nuevo_precio}}
    )
    
    if resultado.modified_count > 0:
        print(f"\nSe actualizó el precio del libro '{titulo_actualizar}' a ${nuevo_precio}")
        # Mostrar el libro actualizado
        libro_actualizado = coleccion.find_one({"titulo": titulo_actualizar})
        print("Libro actualizado:")
        pprint.pprint(libro_actualizado)
    else:
        print(f"\nNo se encontró el libro '{titulo_actualizar}' para actualizar su precio")
    
    # 4. ELIMINAR: Borrar un libro por su título
    titulo_eliminar = "Nada"
    
    resultado = coleccion.delete_one({"titulo": titulo_eliminar})
    
    if resultado.deleted_count > 0:
        print(f"\nSe eliminó el libro '{titulo_eliminar}'")
    else:
        print(f"\nNo se encontró el libro '{titulo_eliminar}' para eliminar")
    
    # 5. LEER: Mostrar todos los libros ordenados por año
    print("\nLibros ordenados por año de publicación:")
    for libro in coleccion.find().sort("año", 1):  # 1 para ascendente
        print(f"{libro['año']} - '{libro['titulo']}' por {libro['autor']} (Editorial: {libro['editorial']}, {libro['precio']}€)")
    
    # 6. LEER: Encontrar el libro más antiguo
    libro_mas_antiguo = coleccion.find_one(sort=[("año", 1)])
    print("\nLibro más antiguo:")
    pprint.pprint(libro_mas_antiguo)
    
    # 7. Búsqueda avanzada: Libros de una editorial específica con precio menor a cierto valor
    editorial = "Planeta"
    precio_maximo = 25.00
    
    print(f"\nLibros de editorial {editorial} con precio menor a {precio_maximo}€:")
    for libro in coleccion.find({"editorial": editorial, "precio": {"$lt": precio_maximo}}):
        pprint.pprint(libro)
```

Este ejercicio práctico abarca todas las operaciones CRUD básicas y algunas consultas más avanzadas, mostrando cómo trabajar con una colección de libros en MongoDB desde Python.

## Demo Avanzada: Análisis de Ventas

En esta sección presentamos una demo completa que muestra cómo utilizar MongoDB con Python para crear un sistema de análisis de datos de productos, tiendas y ventas. Esta demo incluye:

1. Creación y carga de datos de prueba
2. Realización de consultas avanzadas 
3. Uso del framework de agregación para análisis

### Preparación de Datos

Primero, vamos a crear las colecciones y cargar datos de ejemplo:

```python
from pymongo import MongoClient
import re
import random
from datetime import datetime, timedelta
from contextlib import contextmanager

@contextmanager
def get_mongo_client(connection_string):
    client = MongoClient(connection_string)
    try:
        yield client
    finally:
        print('Cerrando conexión...')
        client.close()

# Establecer conexión a MongoDB
try:
    client = MongoClient(
        host='localhost',
        port=27017,
        username='usuario',
        password='micontraseña',
        authSource='admin'
    )
    
    # Verificar la conexión
    client.admin.command('ping')
    print("Conexión exitosa a MongoDB")
    
    # Seleccionar base de datos
    db = client['analisis_datos']
    
    # 1. CREAR COLECCIÓN DE PRODUCTOS
    productos_collection = db['productos']
    productos_collection.drop()  # Limpiar colección existente
    
    # Crear productos con descripciones detalladas
    productos = [
        {
            "nombre": "Ultrabook Pro X5",
            "categoria": "Portátiles",
            "precio": 1299.99,
            "stock": 25,
            "descripcion": "Portátil ultraligero de alta calidad con procesador i7 y pantalla 4K"
        },
        {
            "nombre": "Monitor UltraWide 34",
            "categoria": "Monitores",
            "precio": 449.99,
            "stock": 15,
            "descripcion": "Monitor de alta resolución con excelente calidad de imagen y tecnología HDR"
        },
        {
            "nombre": "Teclado Mecánico RGB",
            "categoria": "Periféricos",
            "precio": 129.99,
            "stock": 50,
            "descripcion": "Teclado mecánico resistente con retroiluminación RGB personalizable"
        },
        {
            "nombre": "Teléfono UltraClear 12",
            "categoria": "Smartphones",
            "precio": 899.99,
            "stock": 30,
            "descripcion": "Smartphone con cámara de alta resolución y batería de larga duración"
        },
        {
            "nombre": "Tarjeta Gráfica Pro Gaming",
            "categoria": "Componentes",
            "precio": 799.99,
            "stock": 10,
            "descripcion": "Tarjeta gráfica de alta calidad para gaming profesional con 12GB VRAM"
        },
        {
            "nombre": "Disco SSD Ultra Rápido",
            "categoria": "Almacenamiento",
            "precio": 199.99,
            "stock": 40,
            "descripcion": "Disco SSD de alta velocidad y resistente para uso intensivo"
        },
        {
            "nombre": "Router Mesh Premium",
            "categoria": "Redes",
            "precio": 249.99,
            "stock": 20,
            "descripcion": "Sistema de red mesh de alta calidad para cobertura completa del hogar"
        }
    ]
    
    # Insertar productos y crear índice de texto en descripción
    productos_collection.insert_many(productos)
    productos_collection.create_index([("descripcion", "text")])
    print(f"Se insertaron {len(productos)} productos y se creó un índice de texto")
    
    # 2. CREAR COLECCIÓN DE TIENDAS CON COORDENADAS GEOESPACIALES
    tiendas_collection = db['tiendas']
    tiendas_collection.drop()  # Limpiar colección existente
    
    # Crear tiendas con ubicaciones geoespaciales
    tiendas = [
        {
            "nombre": "Electrónica Central",
            "direccion": "Calle Mayor 123, Madrid",
            "ubicacion": {
                "type": "Point",
                "coordinates": [-3.7037902, 40.4167754]  # Madrid
            },
            "telefono": "+34 91 123 4567",
            "horario": "9:00-20:00"
        },
        {
            "nombre": "TechShop Barcelona",
            "direccion": "Passeig de Gràcia 101, Barcelona",
            "ubicacion": {
                "type": "Point",
                "coordinates": [2.1686, 41.3874]  # Barcelona
            },
            "telefono": "+34 93 456 7890",
            "horario": "10:00-21:00"
        },
        {
            "nombre": "Gadget Store Valencia",
            "direccion": "Avda. del Puerto 27, Valencia",
            "ubicacion": {
                "type": "Point",
                "coordinates": [-0.3773900, 39.4699075]  # Valencia
            },
            "telefono": "+34 96 345 6789",
            "horario": "9:30-20:30"
        },
        {
            "nombre": "TechCorner Sevilla",
            "direccion": "Calle Sierpes 45, Sevilla",
            "ubicacion": {
                "type": "Point",
                "coordinates": [-5.9953403, 37.3890924]  # Sevilla
            },
            "telefono": "+34 95 567 8901",
            "horario": "10:00-20:00"
        },
        {
            "nombre": "Electrobox Bilbao",
            "direccion": "Gran Vía 76, Bilbao",
            "ubicacion": {
                "type": "Point", 
                "coordinates": [-2.9350039, 43.2630126]  # Bilbao
            },
            "telefono": "+34 94 678 9012",
            "horario": "9:00-21:00"
        }
    ]
    
    # Insertar tiendas y crear índice geoespacial
    tiendas_collection.insert_many(tiendas)
    tiendas_collection.create_index([("ubicacion", "2dsphere")])
    print(f"Se insertaron {len(tiendas)} tiendas y se creó un índice geoespacial")
    
    # 3. CREAR COLECCIÓN DE VENTAS CON DATOS TEMPORALES
    ventas_collection = db['ventas']
    ventas_collection.drop()  # Limpiar colección existente
    
    # Generar datos de ventas para los últimos 2 años
    fecha_inicio = datetime.now() - timedelta(days=730)  # 2 años atrás
    regiones = ["Norte", "Sur", "Este", "Oeste", "Centro"]
    estados = ["completado", "pendiente", "cancelado"]
    
    ventas = []
    for _ in range(200):  # Generar 200 ventas
        # Seleccionar un producto aleatorio
        producto = random.choice(productos)
        
        # Generar fecha aleatoria en los últimos 2 años
        dias_aleatorios = random.randint(0, 730)
        fecha_venta = fecha_inicio + timedelta(days=dias_aleatorios)
        
        # Crear documento de venta
        venta = {
            "producto_id": str(producto["_id"]),
            "nombre_producto": producto["nombre"],
            "categoria_producto": producto["categoria"],
            "monto": producto["precio"] * random.randint(1, 3),  # Venta de 1-3 unidades
            "cantidad_productos": random.randint(1, 3),
            "fecha_venta": fecha_venta,
            "region_cliente": random.choice(regiones),
            "estado": random.choices(estados, weights=[0.8, 0.15, 0.05])[0],  # 80% completadas
            "cliente_id": f"CLI{random.randint(1000, 9999)}"
        }
        ventas.append(venta)
    
    # Insertar ventas
    ventas_collection.insert_many(ventas)
    print(f"Se insertaron {len(ventas)} ventas")
    
except Exception as e:
    print(f"Error: {e}")
finally:
    # Cerrar la conexión
    if 'client' in locals():
        client.close()
        print("\nConexión cerrada")
```

### Consultas Avanzadas

Una vez que los datos están cargados, podemos ejecutar diversas consultas avanzadas:

#### 1. Buscar productos que contengan un patrón en su nombre usando expresiones regulares:

```python
import re

with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    regex_pattern = re.compile('ultra', re.IGNORECASE)
    
    # Importante: Convertir el cursor a lista o almacenar los resultados
    # para evitar que se agote el cursor antes de usarlo
    productos = db.productos.find({"nombre": regex_pattern})
    
    print("\n1. Productos que contienen 'Ultra' en su nombre:")
    if productos:
        for producto in productos:
            print(f"- {producto['nombre']} (${producto['precio']})")
    else:
        print("No se encontraron productos con ese patrón.")
```

**Resultado:**
```
1. Productos que contienen 'Ultra' en su nombre:
- Ultrabook Pro X5 ($1299.99)
- Monitor UltraWide 34 ($449.99)
- Teléfono UltraClear 12 ($899.99)
- Disco SSD Ultra Rápido ($199.99)
```

#### 2. Buscar tiendas cercanas usando coordenadas geoespaciales:

```python
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    # Buscar tiendas cercanas a Madrid (radio de 300km)
    tiendas_cercanas = db.tiendas.find({
    "ubicacion": {
        "$near": {
        "$geometry": {
            "type": "Point",
            "coordinates": [-3.7037902, 40.4167754]  # Madrid
        },
        "$maxDistance": 300_000  # 300km
        }
    }
    })
    
    print("\n2. Tiendas cercanas a Madrid (radio de 300km):")
    for tienda in tiendas_cercanas:
        print(f"- {tienda['nombre']} ({tienda['direccion']})")
```

**Resultado:**
```
2. Tiendas cercanas a Madrid (radio de 300km):
- Electrónica Central (Calle Mayor 123, Madrid)
```

#### 3. Búsqueda de texto en descripciones:

```python
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    # Buscar productos que mencionan "alta calidad" en su descripción
    productos = db.productos.find({"$text": {"$search":"alta calidad"}})
    
    print("\n3. Productos relacionados con 'alta calidad':")
    for producto in productos:
        print(f"- {producto['nombre']}: {producto['descripcion']}")
```

**Resultado:**
```
3. Productos relacionados con 'alta calidad':
- Disco SSD Ultra Rápido: Disco SSD de alta velocidad y resistente para uso intensivo
- Tarjeta Gráfica Pro Gaming: Tarjeta gráfica de alta calidad para gaming profesional con 12GB VRAM
- Teléfono UltraClear 12: Smartphone con cámara de alta resolución y batería de larga duración
- Ultrabook Pro X5: Portátil ultraligero de alta calidad con procesador i7 y pantalla 4K
- Router Mesh Premium: Sistema de red mesh de alta calidad para cobertura completa del hogar
- Monitor UltraWide 34: Monitor de alta resolución con excelente calidad de imagen y tecnología HDR
```

### Análisis con el Framework de Agregación

#### 1. Análisis de ventas por categoría:

```python
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    pipeline = [
        # Etapa 1: Filtrar solo ventas completadas
        {"$match": {"estado": "completado"}},

        # Etapa 2: Agrupar por categoría
        {"$group": {
            "_id": "$categoria_producto",
            "total_ventas": {"$sum": "$monto"},
            "cantidad_ventas": {"$sum": 1},
            "promedio_venta": {"$avg": "$monto"}
        }},

        # Etapa 3: Ordenar por total de ventas
        {"$sort": {"total_ventas": -1}}
    ]

    resultados = db.ventas.aggregate(pipeline)
    
    print("\n--- FRAMEWORK DE AGREGACIÓN ---")
    print("\n1. Análisis de ventas por categoría:")
    for resultado in resultados:
        print(f"Categoría: {resultado['_id']}")
        print(f"  Total ventas: ${resultado['total_ventas']:,.2f}")
        print(f"  Cantidad: {resultado['cantidad_ventas']}")
        print(f"  Promedio: ${resultado['promedio_venta']:,.2f}")
```

**Resultado:**
```
1. Análisis de ventas por categoría:
Categoría: Portátiles
  Total ventas: $59,799.54
  Cantidad: 26
  Promedio: $2,299.98
Categoría: Componentes
  Total ventas: $37,599.53
  Cantidad: 25
  Promedio: $1,503.98
Categoría: Smartphones
  Total ventas: $36,899.59
  Cantidad: 21
  Promedio: $1,757.12
Categoría: Monitores
  Total ventas: $26,999.40
  Cantidad: 27
  Promedio: $999.98
Categoría: Redes
  Total ventas: $9,499.62
  Cantidad: 20
  Promedio: $474.98
Categoría: Periféricos
  Total ventas: $6,369.51
  Cantidad: 26
  Promedio: $244.98
Categoría: Almacenamiento
  Total ventas: $6,199.69
  Cantidad: 15
  Promedio: $413.31
```

#### 2. Análisis avanzado: Ventas por región y tiempo:

```python
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    pipeline = [
        # Etapa 1: Añadir campos calculados para fecha
        {"$addFields": {
            "mes": {"$month": "$fecha_venta"},
            "año": {"$year": "$fecha_venta"}
        }},

        # Etapa 2: Agrupar por región, año y mes
        {"$group": {
            "_id": {
                "region": "$region_cliente",
                "año": "$año",
                "mes": "$mes"
            },
            "total_ventas": {"$sum": "$monto"},
            "cantidad_transacciones": {"$sum": 1},
            "productos_vendidos": {"$sum": "$cantidad_productos"}
        }},

        # Etapa 3: Agrupar de nuevo para obtener totales anuales por región
        {"$group": {
            "_id": {
                "region": "$_id.region",
                "año": "$_id.año"
            },
            "ventas_mensuales": {"$push": {
                "mes": "$_id.mes",
                "ventas": "$total_ventas",
                "transacciones": "$cantidad_transacciones"
            }},
            "total_anual": {"$sum": "$total_ventas"},
            "promedio_mensual": {"$avg": "$total_ventas"}
        }},
        
        # Etapa 4: Ordenar por región y año
        {"$sort": {"_id.region": 1, "_id.año": 1}}
    ]

    resultados = db.ventas.aggregate(pipeline)

    # Mostrar solo un ejemplo resumido para no extender demasiado la salida
    print("\n2. Análisis de ventas por región y tiempo (ejemplo para región Centro):")
    for resultado in resultados:
        region = resultado["_id"]["region"]
        año = resultado["_id"]["año"]
        
        if region == "Centro":  # Mostrar solo datos de la región Centro como ejemplo
            print(f"Región: {region}, Año: {año}")
            print(f"  Total anual: ${resultado['total_anual']:,.2f}")
            print(f"  Promedio mensual: ${resultado['promedio_mensual']:,.2f}")
            print("  Desglose mensual:")
            for mes in sorted(resultado["ventas_mensuales"], key=lambda x: x["mes"])[:3]:  # Mostrar solo los primeros 3 meses
                print(f"    Mes {mes['mes']}: ${mes['ventas']:,.2f} ({mes['transacciones']} transacciones)")
            print()
```

**Resultado parcial (para Centro):**
```
2. Análisis de ventas por región y tiempo (ejemplo para región Centro):
Región: Centro, Año: 2023
  Total anual: $13,709.74
  Promedio mensual: $2,741.95
  Desglose mensual:
    Mes 7: $459.97 (2 transacciones)
    Mes 8: $3,449.94 (2 transacciones)
    Mes 9: $4,899.93 (4 transacciones)

Región: Centro, Año: 2024
  Total anual: $26,379.51
  Promedio mensual: $2,398.14
  Desglose mensual:
    Mes 1: $1,999.97 (2 transacciones)
    Mes 2: $599.97 (1 transacciones)
    Mes 3: $199.99 (1 transacciones)

Región: Centro, Año: 2025
  Total anual: $8,649.88
  Promedio mensual: $2,883.29
  Desglose mensual:
    Mes 1: $3,899.97 (1 transacciones)
    Mes 2: $2,249.95 (2 transacciones)
    Mes 3: $2,499.96 (2 transacciones)
```

### Análisis de Productos por Valoración

En este último ejemplo, creamos una nueva colección de productos con datos aleatorios y realizamos un análisis de valoración por categoría:

```python
from datetime import datetime
import random

# Crear colección de productos con valoraciones
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    productos = db["productos"]

    productos.drop()

    categorias = ["Electrónica", "Ropa", "Hogar", "Deportes", "Libros"]
    nombres_productos = [
        ["Smartphone", "Laptop", "Auriculares", "Tablet", "Smartwatch"],
        ["Camiseta", "Pantalón", "Zapatillas", "Abrigo", "Gorra"],
        ["Lámpara", "Sofá", "Mesa", "Silla", "Estantería"],
        ["Balón", "Raqueta", "Bicicleta", "Mancuernas", "Patines"],
        ["Novela", "Ensayo", "Biografía", "Cómic", "Manual"]
    ]

    # Insertar 50 productos aleatorios
    productos_a_insertar = []
    for _ in range(50):
        cat_index = random.randint(0, 4)
        categoria = categorias[cat_index]
        nombre = random.choice(nombres_productos[cat_index])
        modelo = f"Modelo-{random.randint(1, 100)}"
        
        productos_a_insertar.append({
            "nombre": f"{nombre} {modelo}",
            "categoria": categoria,
            "precio": round(random.uniform(10, 1000), 2),
            "stock": random.randint(0, 100),
            "valoracion": round(random.uniform(1, 5), 1),  # Valoración de 1 a 5
            "num_valoraciones": random.randint(1, 500),
            "fecha_alta": datetime(2023, random.randint(1, 12), random.randint(1, 28))
        })

    productos.insert_many(productos_a_insertar)

# Análisis de productos por valoración
with get_mongo_client("mongodb://usuario:micontraseña@localhost:27017/") as client:
    db = client.analisis_datos
    
    pipeline = [
        # Etapa 1: Agrupar por categoría
        {"$group": {
            "_id": "$categoria",
            "num_productos": {"$sum": 1},
            "valoracion_media": {"$avg": "$valoracion"},
            "precio_medio": {"$avg": "$precio"},
            "stock_total": {"$sum": "$stock"}
        }},
        
        # Etapa 2: Ordenar por valoración media (descendente)
        {"$sort": {"valoracion_media": -1}}
    ]

    resultados = db.productos.aggregate(pipeline)

    print("\n3. Categorías de productos por valoración media:")
    print("=============================================")
    for resultado in resultados:
        print(f"Categoría: {resultado['_id']}")
        print(f"  Valoración media: {resultado['valoracion_media']:.1f}/5.0")
        print(f"  Número de productos: {resultado['num_productos']}")
        print(f"  Precio medio: ${resultado['precio_medio']:.2f}")
        print(f"  Stock total: {resultado['stock_total']} unidades")
        print()
```

**Resultado:**
```
3. Categorías de productos por valoración media:
=============================================
Categoría: Ropa
  Valoración media: 3.3/5.0
  Número de productos: 13
  Precio medio: $545.56
  Stock total: 534 unidades

Categoría: Electrónica
  Valoración media: 2.7/5.0
  Número de productos: 9
  Precio medio: $560.22
  Stock total: 444 unidades

Categoría: Deportes
  Valoración media: 2.6/5.0
  Número de productos: 9
  Precio medio: $496.84
  Stock total: 494 unidades

Categoría: Hogar
  Valoración media: 2.5/5.0
  Número de productos: 8
  Precio medio: $419.22
  Stock total: 442 unidades

Categoría: Libros
  Valoración media: 2.5/5.0
  Número de productos: 11
  Precio medio: $694.30
  Stock total: 676 unidades
```

