# Curso Data Engineering - UTN | 30/11/2023
### TP integrador: datos meteorológicos ###

- **Alumno:** Gabriel Molina  
- **APIs elegidas:** 
    - Open-meteo Weather Forecast API
    - Open-meteo Geocoding API

> **Sobre APIs** https://open-meteo.com/  
> Las múltiples API que ofrece **open-meteo** funcionan pasando los parámetros *en* el URL.

**NOTA:** se intentó implementar una funcionalidad (opcional) de automatización para esta extracción.

## Entrega definitiva: Extracción + Almacenamiento (Data Lake) + **Precesamiento** (y carga en Datawarehause posgreSQL)
### NOTAS sobre ejecución

#### Información e interacción `por terminal`
> Con la esperanza de facilitar la revisión de esta entrega, luego de imprimir por terminal los datos de la ciudad elegida como ejemplo (La Plata, Argentina), es posible decidir  si efectuar la extracción automatizada, o solo un registro (como demostración) al ejecutar `extrac_almac.py`.

#### Módulos
- **main.py** : Contientiene las declaraciones de las clases `DataLake, Extrac, Autom ` y `PgSql` con la lógica para el _data pipe_. Estos se importan e instancian en los otros módulos.
- **extrac_almac.py** : Controla los pasos de extracción (`Extrac`) y almacenamiento (`DataLake`). Automatización opcional (`Autom`)
- **tranf.py** : Procedimiento de carga de datos brutos desde el *Data Lake* (`DataLake`), ***transformación*** y almacenamiento (`PgSql`) en *DataWarehouse* (base de datos _'ORION'_)  
 -- **NOTA:** al ejecutar se informa sobre el avance del proceso por terminal.

   
**Archivo de configuración `config.ini`**: Contiene los URLs para las API, y las credenciales de conexión con la base de Datos postgreSQL asignada (_ORION_).

**Secciones y variables almacenadas:**

    [endpoint]
    tiempo = URL API open-meteo
    localidad = URL geocoding-API

    [parametros]
    tiempo_actual = cadena de parámetros como se indica en documentación de API open-meteo

    [postgres_tpint]
    alias = alias base de datos
    host = host base de datos
    port = puerto base de datos
    db = nombre base de datos
    user = usuario base de datos
    pwd = contraseña base de datos
    schema = esquema base de datos

### Extracción batch full (Geocoding API)

Se extrae resultados de búsqueda por nombre de ciudad. Por defecto se limitó el número de resultados a 10. Las coordenadas geográficas son un parámetro primordial para poder acceder a los datos meteorológicos. La localidad elegida es "La Plata, Buenos Aires, Argentina", sin embargo, se almacenan todos los resultados descargados en un único archivo `.parquet`.

NOTA: si bien todo está orquestado para La Plata, se agregó lógica para que no hubiesen conflictos si la ciudad se cambiara. Además se controla que no se repita registros en el Data Lake.

### Extracción batch incremental (Weather Forecast API)

NOTA: Automaticamente se toman las coordenadas del primer resultado de la búqueda de localidad. 

Se almacenan las variables meteorológicas correspondientes a las coordenadas especificadas, y desde el momento de ejecución en adelante. El modelo meteorológico de la API produce datos cada 900 segundos (15 minutos). Cada registro es agregado como fila al dataframe (objeto pandas). Como se está confeccionando un historial de condiciones ambientales, no hace falta verificar los datos anteriores al momento de la inserción (no hay información "redundante").

## Resumen transformación de datos

### Datos meteorológicos
- Eliminar columna de partición para _Data lake_
- Hora de GMT-0 a GMT-3 (hora local Argentina)
- Formateo de fecha, y separación en fecha y hora (str / object)
- Nueva columna ('winddir_cardinal_10m') de Dirección del viento por puntos cardinales
- 'pressure_msl' transformada de hPa a mmHg
- Transformar 'is_day' a booleana
- Traer ciudad y país desde tabla localidades (`left join`)

### Datos localidad
- Formatear 'postcodes'
- Pasar None a NaN

## Carga a Data Warehouse
> **NOTA**  
Para los propósitos del curso para el que fue desarrollada esta aplicación, se empleó un servicio gratuito provisto por [Aiven](https://aiven.io/) para crear una base de datos online que funcionara como Data Warehouse para el ejercicio.

Se emplean credenciales de base de datos proporcionadas, almacenadas en `config.ini`, para crear las tablas:
> **Esquema `molina_gabriel`**
> - `meteor_proc` (datos meteorológicos) clave primaria compuesta:  `date` y `time`
> - `loc_proc` (datos de localidad) clave primaria: id de ciudad (tomada de API)
> - `meteor_proc_stg` tabla _stage_ o de espera (ver abajo) para evitar repetición de registros.
> - `loc_proc_stg` tabla _stage_ o de espera  para aplicar SCD1 (ver abajo).

### Estrategia de carga de datos
- Para `meteor_proc` se usa una tabla _stage_ con el objetivo de comparar claves primarias entre un registro entrante y los ya almacenados en la tabla. Esto evita repetición de registros si se ejecuta **tranf.py** más de una vez.
- Para `loc_proc` se ejecuta la estrategia _Slowly Changing Dimensions_ tipo 1. Con una columna `fecha_actualizacion` que informa la última vez que se modificó el registro y una `fecha_actualizacion_origen` con la fecha de creación del mismo.



## Documentación del código
> **Módulos de terceros importados**
> - os  
> - configparser  
> - requests
> - pandas
> - time
> - threading
> - sqlalchemy
> - numpy


> **NOTA**: Comentarios en los scripts sobre parámetros de cada método.

### clase Extrac
Carga los *endpoints* almacenados en el archivo "endpoints.cfg" (debe guardarse en directorio de ejecución).

Toma como argumentos (obtenidos con búsqueda de Geocoding API): la id de la localidad, la latitud y la longitud.

Al instanciarse la clase Almacena tablas vacías con las variables entregadas por la API como columnas.  
Métodos:  
- **ciudad_df** (método de clase) devuelve datos sobre la ciudad ingresada como parámetro. NOTA: se debió agregar lógica que estandarice los campos, debido a que la API elimina las columnas vacías de la respuesta y esto puede conducir a error.

- **regist_tiempo_df** guarda la información meteorológica disponible para el momento cada vez que es ejecutada. Se crea la nueva columna `fecha_partic` para el posterior guardado particionado por fecha en el Data Lake.
- **_pedido_tiempo** bloque que ejecuta la consulta a la API según el endopoint especificado.
- **impr_registros** imprime en terminal toda la tabla almacenada en instancia.  

### clase Autom
Recibe objeto de clase Extrac al instanciarse.  
Métodos:  
- **_bucle_descarga** bucle continuo de pedidos a API, según intervalo de tiempo (en segundos)

- **lanzar_descarga** lanza _bucle_descarga en un hilo independiente, el cual se puede detener por consola.

### clase DataLake
Crea estructura de directorios del data lake al instanciarse.

    meteor_data (dir. datalake)
        └ landing (capa de datos brutos)
            └ localid (dir. localidades)
            └ regmeteor (dir. registros meteor. partic: fecha)

Métodos:  
- **a_parquet_inc** permite guardar los registros en capa landing/regmeteor. Agrega registros con fecha como partición dentro del directorio. Controla que no se repitan las filas (puede ocurrir si ejecuta el modo descarga única más de una vez en menos de 15 minutos).
- **a_parquet_full** guarda, en capa landing/localid. Controla que no se repitan las filas.
- **leer_parq** permite cargar registros previos desde el datalake.

### clase PgSql
Al instanciarse recupera las credenciales almacenadas en archivo de configuración (`config.ini`), y las emplea para crear motor de base de datos con `sqlalchemy`. Si el esquema proporcionado no existe, lo crea.

Métodos:
- **crear_tb** Crea tabla con el nombre, columnas y tipos de datos proporcionados en la base postgreSQL conectada con el motor, si esta no existe.
- **cargar_df** Inserta todos los registros de un dataframe en una tabla con los mismos campos. NOTA: Emplea el método `append` de pandas.
- **impr_tabla** Carga toda la tabla especificada por nombre en la memoria como dataframe y la imprime por terminal. (Esto es más que bada para demostrar que todo está funcionando).
- **ejec_query** Envía una consulta SQL a la base de datos.
