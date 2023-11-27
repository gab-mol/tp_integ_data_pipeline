# Curso Data Engineering - UTN | 22/11/2023
### TP integrador: datos meteorológicos ###

- **Alumno:** Gabriel Molina  
- **APIs elegidas:** 
    - Open-meteo Weather Forecast API
    - Open-meteo Geocoding API

> **Sobre APIs** https://open-meteo.com/  
> Las múltiples API que ofrece **open-meteo** funcionan pasando los parámetros *en* el URL.

**NOTA:** se intentó implementar una funcionalidad (opcional) de automatización para esta extracción.

## Tercera Entrega: Extracción + Almacenamiento + **Precesamiento**
### NOTAS sobre ejecución

#### Información e interacción `por terminal`
> Con la esperanza de facilitar la revisión de esta entrega, luego de imprimir por terminal los datos de la ciudad elegida como ejemplo (La Plata, Argentina), es posible decidir  si efectuar la extracción automatizada, o solo un registro como muestra al ejecutar `extrac_almac.py`.

#### Módulos
- **main.py** : Contientiene las declaraciones de las clases `DataLake, Extrac, Autom ` y `PgSql` con la lógica para la tubería de datos. Estos se importan e instancian en los otros módulos.
- **extrac_almac.py** : Controla los pasos de extracción (`Extrac`) y almacenamiento (`DataLake`). Automatización opcional (`Autom`)
- **tranf.py** : Procedimiento de carga de datos brutos desde el *Data Lake* (`DataLake`), ***transformación*** y almacenamiento (`PgSql`) en *DataWarehouse* (base de datos _'ORION'_)  
 -- **NOTA:** al ejecutar se informa del proceso por terminal, sobre el avance del proceso.

   
**Archivo de configuración `config.ini`**: Contiene los URLs para las API, y las credenciales de conexión con la base de Datos postgreSQL asignada (_ORION_).

#### Extracción batch full (Geocoding API)

Se extrae resultados de búsqueda por nombre de ciudad. Las coordenadas geográficas son un parámetro primordial para poder acceder a los datos meteorológicos.

#### Extracción batch incremental (Weather Forecast API)

Se almacenan las variables del tiempo correspondientes a las coordenadas especificadas y desde el momento de ejecución en adelante. El modelo meteorológico de la API produce datos cada 900 segundos (15 minutos). Cada registro es agregado como fila al dataframe (objeto pandas). Como se está confeccionando un historial de condiciones ambientales, no hace falta verificar los datos anteriores al momento de la inserción (no hay información "redundante").

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
Se emplean credenciales de base de datos proporcionadas, almacenadas en `config.ini`, para crear las tablas:
> **Esquema `molina_gabriel`**
> - `meteor_proc` (datos meteorológicos) clave primaria: autoincremental  
> - `loc_proc` (datos de localidad) clave primaria: id de ciudad (tomada de API)

## Documentación del código
> **Módulos de terceros importados**
> - os  
> - configparser  
> - requests
> - pandas
> - time
> - threading
> - sqlalchemy

### clase Extrac
Carga los *endpoints* almacenados en el archivo "endpoints.cfg" (debe guardarse en directorio de ejecución).

Al instanciarse la clase Almacena tablas vacías con las variables entregadas por la API como columnas.  
Métodos:  
- **ciudad_df** (método de clase) devuelve datos sobre la ciudad ingresada como parámetro.

- **regist_tiempo_df** guarda la información meteorológica disponible para el momento cada vez que es ejecutada. La columna "time" es reemplezada por las columnas "fecha", "hora" y "min". Las primeras dos sirven para particionar el registro.  
NOTA: la hora es proporcionada por el endpoint en GMT+0, por lo que es ajustada a GMT-3 (Hora correspondiente a Argentina).
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
- **a_parquet_met** permite guardar los registros en capa landing/regmeteor. Agrega registros con fecha como partición dentro del directorio. Controla que no se repitan las filas (puede ocurrir si ejecuta el modo descarga única más de una vez en menos de 15 minutos).
- **a_parquet** guarda, en capa landing/localid. Sobrescribe datos. **sin llevar un control** de las actualizaciones de proporcione la API.
- **leer_parq** permite cargar registros previos desde el datalake.

### clase PgSql
Al instanciarse recupera las credenciales almacenadas en archivo de configuración (`config.ini`), y las emplea para crear motor de base de datos con `sqlalchemy`.

Métodos:
- **crear_tb** Crea tabla con el nombre proporcionado en la base postgreSQL conectada con el motor, si esta no existe.
- **cargar_df** Inserta todos los registros de un dataframe en una tabla con los mismos campos. Reemplaza todo registro previo en dicha tabla.
- **impr_tabla** Carga toda la tabla especificada por nombre en la memoria como dataframe y la imprime por terminal.

