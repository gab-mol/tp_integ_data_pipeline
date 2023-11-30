'''
### Tercera entrega: Curso Data Engineer - UTN ###
    Clases con lógica para Data Pipe Line - 

Alumno: Molina Gabriel
'''

import os
import configparser
import requests
import pandas
import time
import threading
import sqlalchemy
from numpy import nan

RUTA_DIR = os.path.dirname(__file__)
SQL_DB = "postgres_tpint"

class DataLake:

    '''Crea estructura de dirs del datalake. 
    Permite almecenar registros. Lee el archivo 
    **config.ini** del directorio de trabajo. '''

    # Verificar existencia de dir del lake
    DIR_LAND_MET = os.path.join(
        RUTA_DIR, "meteor_data\\landing\\regmeteor"
    )

    DIR_LAND_CIUD = os.path.join(
        RUTA_DIR, "meteor_data\\landing\\localid"
    )

    if not os.path.exists(DIR_LAND_MET): 
        os.makedirs(DIR_LAND_MET)

    if not os.path.exists(DIR_LAND_CIUD): 
        os.makedirs(DIR_LAND_CIUD)

    arch_loc = os.path.join(DIR_LAND_CIUD,"localidades.parquet")

    def __init__(self) -> None:
        pass

    @classmethod
    def leer_parq(self, opc:str) -> pandas.DataFrame:
        '''Leer datos desde el Data Lake.

        args
            opc: {"regmeteor", "localid"} 
                regmeteor: para registro meteorol.
                localid: para ciudades.
        '''

        if opc== "regmeteor":
            try:
                dat = pandas.read_parquet(self.DIR_LAND_MET)
            except:
                dat = None
        elif opc=="localid":
            try:            
                dat = pandas.read_parquet(self.arch_loc)
            except:
                dat = None
        else:
            raise Exception("Opción inexistente")

        # Evitar error primera ejecución
        if dat is None:
            print("\nSIN registros previos")

        # Si df vacío, -> None    
        elif dat.empty:
            print("\nSIN registros previos")
            return None

        return dat

    def a_parquet_inc(self, registro:pandas.DataFrame, partic:list, 
            adv=True):

        '''Guardar dataframe como parquet en datalake.
        
        args
            registro: `pandas.DataFrame` con registro nuevo
            partic: lista con columnas para partición
            adv: controla aviso de repetición de filas
            '''
        
        # Control primer rgistro
        if os.listdir(self.DIR_LAND_MET) == []:
            registro.to_parquet(
                path=self.DIR_LAND_MET, 
                append=False,
                engine="fastparquet",
                partition_cols=partic
                )
        else:
            # Prevenir registros repetidos
            prev = DataLake.leer_parq("regmeteor")
            
            com_fec = str(registro['time'].iloc[-1]) == str(prev['time'].iloc[-1])
            
            if com_fec:
                if adv: print("ADVERTENCIA: REGISTRO METEOROLÓGICO \
REPETIDO \n(ESPERAR NUEVOS DATOS DESDE API - cada 900 seg - )")
            else:
                registro.to_parquet(
                path=self.DIR_LAND_MET, 
                append=True,
                engine="fastparquet",
                partition_cols=partic
                )

    def a_parquet_full(self, registro:pandas.DataFrame):

        '''Guardado en `.parquet`. 
        Este método está destinado a la extracción batch full.

        args
            registro: pandas.DataFrame a guardar en `.parquet`
                ruta automaticamente tomada de constante `DIR_LAND_CIUD`
                (>> Data Lake)
        '''
        # Prevenir registros repetidos
        prev = DataLake.leer_parq("localid")

        rep = []

        if prev is not None:
            for r in list(registro['id']):
                if r in list(prev['id']): rep.append(True)

        # Evitar error 'FileNotFoundError'
        if not os.path.exists(self.arch_loc):
            append = False
        else:
            append = True

        # Guardado en parquet
        if True not in rep:
            registro.to_parquet(
                path=self.arch_loc,
                append=append,
                engine="fastparquet"
            )
        else:
            print("\nAVISO: Resultado de búsqueda de localidades ya \
almacenado en Data Lake\n")


class Extrac:
    
    '''Extracción: URL almacenados en archivo de configuración 
    `config.ini`.

    args
        id: identificador de la localidad 
        latitud: latitud de localidad 
        longitud: longitud de localidad 
    '''
    
    RUTA_CFG = os.path.join(RUTA_DIR, "config.ini")
    config = configparser.ConfigParser()
    config.read(RUTA_CFG)

    @staticmethod
    def _pedido_tiempo(endpoint:str) -> dict:

        '''Hacer pedido a API.
        
        args
            endpoint: endpoint de la API.
        '''

        try:
            respuesta = requests.get(endpoint)
        except:
            respuesta = None
            raise Exception("Error de conexión: Weather Forecast API")
        
        dic_tiempo = respuesta.json()

        if "error" in list(dic_tiempo.keys()): 
            raise Exception("Error en respuesta de API")

        return dic_tiempo

    def __init__(self, id:int, latitud:float, longitud:float):
        
        self.api_loc_id = id
        self.latitud = latitud
        self.longitud = longitud

        API = self.config["endpoint"]["tiempo"]
        ubic = f"latitude={self.latitud}&longitude={self.longitud}"
        param = self.config["parametros"]["tiempo_actual"]
        self.ENDPOINT_TIEMPO = API+ubic+param

    def regist_tiempo_df(self) -> pandas.DataFrame:
        
        '''Extracción. Variables de meteorológicas 
        actuales en localidad.'''

        # Pedido a API
        dic_tiempo = Extrac._pedido_tiempo(self.ENDPOINT_TIEMPO)

        # A dataframe. Separar col 'time' a 'fecha' y 'hora:min'
        registro_df = pandas.json_normalize(dic_tiempo["current"])
        registro_df.time = pandas.to_datetime(registro_df.time)
        
        registro_df.insert(0, "fecha_partic", registro_df.time.dt.strftime('%m-%d-%y'))
        
        registro_df.insert(0, "api_loc_id", self.api_loc_id)

        return registro_df
    
    @classmethod
    def ciudad_df(self, ciudad:str, nres=1) -> pandas.DataFrame:
        
        '''Búsqueda datos ciudad por nombre.\n
        :ciudad: nombre en español.
        :nres: número de resultados deseados. defoult=1'''

        API = self.config["endpoint"]["localidad"]
        ciudad = ciudad.replace(" ", "+")
        param = f"name={ciudad}&count={nres}&language=es&format=json"
        ENDPOINT_LOC = API+param
        
        try:
            respuesta = requests.get(ENDPOINT_LOC)
        except:
            respuesta = None
            raise Exception("Error de conexión: Geocoding API")

        dic_ciud = respuesta.json()
        
        result = pandas.json_normalize(dic_ciud["results"])

        # API BORRA los campos vacíos del json: agregar nulos para consitencia
        max_campos = ['id', 'name', 'latitude', 'longitude', 'elevation',
            'feature_code', 'country_code', 'admin1_id', 'admin2_id', 
            'admin3_id', 'admin4_id', 'timezone', 'population', 'postcodes', 
            'country_id', 'country', 'admin1', 'admin2', 'admin3', 'admin4']
        
        campos_loc = list(result.columns)

        # Evitar 'TypeError: NoneType Object Is Not Iterable'
        #   (no puedo usar None para rellenar)
        for camp in max_campos:
            if camp not in campos_loc:
                result[camp] = nan
        
        # Evitar errores de tipo de datos
        for c in ["admin1_id","admin2_id","admin3_id","admin4_id"]:
            result[c] = result[c].astype(str)

        for c in ["admin1","admin2","admin3","admin4"]:
            result[c] = result[c].astype(str)

        # reordenar
        result = result[max_campos]

        return result


class Autom:
    
    '''Función - opcional - de automatización de extracción.
    
    args
        extract: recibe instancia de clase `Extrac`.
        datalake: recibe instancia de clase `DataLake`.
    '''

    estado_des = False

    def __init__(self, extract:Extrac, datalake:DataLake):
        
        self.extract = extract
        self.datalake = datalake
        
    def _bucle_descarga(self, intervalo:int):
        
        '''Método target para el hilo
        args
            intervalo: tiempo en segundos entre ejecuciones del bucle.
            '''

        # Extracción incremental datos de actuales aire y variables meteor.
        while True:
            registro = self.extract.regist_tiempo_df()
            self.datalake.a_parquet_inc(
                registro=registro, 
                partic=["fecha_partic"], 
                adv=False)
            time.sleep(intervalo)
            if self.estado_des:
                break

    def lanzar_descarga(self, intervalo:int):
        
        '''Inicia bucle de descarga.
        
        args
            `intervalo`: tiempo en segundos entre ejecuciones.'''
        
        hilo = threading.Thread(daemon=True, target=self._bucle_descarga, 
            args=(intervalo,))
        hilo.start()
        print("\nSe están guardando datos cada: ", intervalo, "segundos\n")
        elegir = ""
        while True:
            if elegir == "stop":
                self.estado_des = False
                print("\n******\nRegistros almacenados:\n", 
                    DataLake.leer_parq("regmeteor"),
                    "\n******")
                print("\nExtracción detenida. \
Reinicie aplicación para reanudar.\n")
                break
            else:
                elegir = input("¿Detener descarga e imprimir \
datos? (ingresar: stop)\n\t> ")


class PgSql:

    '''Conexión con base postgreSQL.
    Lee credenciales de base de datos del archivo `config.ini` 
    en el directorio de trabajo.

    Nota: constante `SQL_DB` controla clave de sección leída del archivo de config.
    (empleada durante el desarrollo).
    '''

    def __init__(self) -> None:
        RUTA_CFG = os.path.join(RUTA_DIR, "config.ini")
        config = configparser.ConfigParser()
        config.read(RUTA_CFG)

        bd_cred = config[SQL_DB]
        usu, cont, host = bd_cred["user"], bd_cred["pwd"], bd_cred["host"]
        puet, self.bd, self.squ = bd_cred["port"], bd_cred["db"], bd_cred["schema"]

        url = f"postgresql://{usu}:{cont}@{host}:{puet}/{self.bd}?sslmode=require"

        self.engine = sqlalchemy.create_engine(url,
                                connect_args={"options": f"-c search_path={self.squ}"}
                                )

        with self.engine.connect() as con:
            if self.squ not in con.dialect.get_schema_names(con):
                print("\nESQUEMA NO EXISTE, CREAR...")
                try:
                    con.execute(sqlalchemy.text(f"CREATE SCHEMA {self.squ}"))
                    con.commit()
                except:
                    raise Exception("ERROR AL CREAR ESQUEMA PARA TABLAS")
    
   
    def crear_tb(self, nomb:str,cols_type:dict, id_auto=True):
        
        '''Crea la tabla en base de datos con el nombre 
        y columnas/tipo dato especificado, si esta NO existe.

        args
            nombre: str nombre de tabla
            cols_type: dict claves= nombre.col / valores = tipo.dato
            id_auto: defoult: True. Controla si clave primaria es autoincremental o
            debe proporcionarse. (obsoleto)'''
        
        D_l = [f"{k} {cols_type[k]}" for k in cols_type]
        cols_q = ",\n".join(D_l)

        if id_auto:
            opc = "ID SERIAL PRIMARY KEY,"
        else:
            opc = ""

        try:
            with self.engine.begin() as con:
                con.execute(sqlalchemy.text(f"CREATE TABLE IF NOT EXISTS {nomb}(\n\
{opc}\n{cols_q})"))
                con.commit()
            print(f"\nTabla: {nomb} (DB: {self.bd}; \
schem: {self.squ}) = DISPONIBLE\n")
        except:
            Exception("PostgreSQL error")
        
    def cargar_df(self, nomb:str, df:pandas.DataFrame, method=None):

        '''Cargar dataframe a tabla especificada.

        args
            nomb: nombre de la tabla.
            df: dataframe a cargar.
            method: {None, 'multi', 'callable'}, defoult: None. 
                Parámetro de `pandas.DataFrame.to_sql`.'''

        try:
            with self.engine.connect() as con, con.begin():
                print("Conectando con postgreSQL...")
                print(df)
                df.to_sql(
                    name=nomb,
                    con=con,
                    schema=self.squ,
                    if_exists="append",
                    index=False,
                    method=method,
                    chunksize=1000
                )
        except:
            raise Exception("Error de carga: pandas.Dataframe >> postgreSQL")

    def ejec_query(self, query:str, commit=True):

        '''Crea conexión con el motor instanciado y 
        envia query.

        args
            query: str con sentencia SQL.
            commit: aplicar método `sqlalchemy.Connection.commit()`
                defoult: True
            '''

        try:
            with self.engine.connect() as con, con.begin():
                con.execute(sqlalchemy.text(query))
                if commit: con.commit()
        except:
            raise Exception("Error al ejecutar sentencia SQL")


    def impr_tabla(self, nomb:str):

        '''Imprime todos los campos y registros de la tabla
        especificada (esta es transformada al completo en un
        `pandas.Dataframe`).
        
        args
            nomb: referenciar por nombre una tabla existente 
            en base de datos.
        '''

        with self.engine.connect() as con:
            df = pandas.read_sql_table(
                table_name=nomb,
                schema=self.squ, 
                con=con)

            print(df)