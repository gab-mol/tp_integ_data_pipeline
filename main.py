'''
### Tercera entrega: Curso Data Engineer - UTN ###

    - Módulo con lógica para pipe line - 

Alumno: Molina Gabriel
'''

import os
import configparser
import requests
import pandas
import time
import threading
import sqlalchemy


RUTA_DIR = os.path.dirname(__file__)
SQL_DB = "postgres_prueba"

class DataLake:

    '''Crea estructura de dirs del datalake. 
    Permite almecenar registros.'''

    # Verificar existencia de dir del lake
    DIR_LAND_MET = os.path.join(
        RUTA_DIR, "meteor_data/landing/regmeteor"
    )

    DIR_LAND_CIUD = os.path.join(
        RUTA_DIR, "meteor_data/landing/localid"
    )

    if not os.path.exists(DIR_LAND_MET): 
        os.makedirs(DIR_LAND_MET)

    if not os.path.exists(DIR_LAND_CIUD): 
        os.makedirs(DIR_LAND_CIUD)

    def __init__(self) -> None:
        pass

    @classmethod
    def leer_parq(self, opc:str) -> pandas.DataFrame:
        '''Leer datos.
        
        :regmeteor: para registro meteorol. 
        :localid: para ciudades.
        '''
        if opc== "regmeteor":
            try:
                dat = pandas.read_parquet(self.DIR_LAND_MET)
            except:
                dat = None
        elif opc=="localid":
            try:            
                dat = pandas.read_parquet(self.DIR_LAND_CIUD)
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

    def a_parquet_met(self, registro:pandas.DataFrame, partic:list, 
            adv=True):

        '''Guardar dataframe como parquet en datalake.
        
        :registro: DataFrame con registro nuevo
        :partic: lista con columnas para partición
        :adv: controla aviso de repetición de filas'''
        
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
                if adv: print("ADVERTENCIA: REGISTRO REPETIDO \n\
(ESPERAR NUEVOS DATOS DESDE API - cada 900 seg - )")
            else:
                registro.to_parquet(
                path=self.DIR_LAND_MET, 
                append=True,
                engine="fastparquet",
                partition_cols=partic
                )

    def a_parquet(self, registro:pandas.DataFrame):

        '''Particiona por ciudad. Sobrescribe tabla previa.'''
        
        archivo = os.path.join(self.DIR_LAND_CIUD,"localidades.parquet")
        
        registro.to_parquet(
            path=archivo,
            append=False,
            engine="fastparquet"
        )


class Extrac:
    
    '''Extracción.'''
    
    RUTA_CFG = os.path.join(RUTA_DIR, "config.ini")
    config = configparser.ConfigParser()
    config.read(RUTA_CFG)

    @staticmethod
    def _pedido_tiempo(endpoint) -> dict:

        '''Hacer pedido a API'''

        try:
            respuesta = requests.get(endpoint)
        except:
            respuesta = None
            raise Exception("Error de conexión: Weather Forecast API")
        
        dic_tiempo = respuesta.json()

        return dic_tiempo

    def __init__(self, id:int, latitud:float, longitud:float):
        
        self.api_loc_id = id
        self.latitud = latitud
        self.longitud = longitud

        API = self.config["endpoint"]["tiempo"]
        ubic = f"latitude={self.latitud}&longitude={self.longitud}"
        param = self.config["parametros"]["tiempo_actual"]
        self.ENDPOINT_TIEMPO = API+ubic+param

    def regist_tiempo_df(self) -> pandas.DataFrame: #h_loc=True
        
        '''Extracción. Variables de meteorológicas 
        actuales en localidad.'''

        # Pedido a API
        dic_tiempo = Extrac._pedido_tiempo(self.ENDPOINT_TIEMPO)

        # A dataframe. Separar col 'time' a 'fecha' y 'hora:min'
        registro_df = pandas.json_normalize(dic_tiempo["current"])
        registro_df.time = pandas.to_datetime(registro_df.time)
        
        #registro_df.insert(0, "hora", pandas.to_datetime(registro_df.time, format="%H:%M"))
        registro_df.insert(0, "fecha_partic", registro_df.time.dt.strftime('%m-%d-%y'))
        
        #registro_df = registro_df.drop(columns=["time"])
        
        # Pasar (manualemente) hora a local argentina: GMT-3
        # if h_loc:
        #     print(registro_df["hora"].iloc[-1],"\n",type(registro_df["hora"].iloc[-1]))
        #     h_loc = registro_df["hora"].iloc[-1] - pandas.Timedelta(hours=3)
        #     registro_df.at[0,'hora'] = h_loc

        # Columna ciudad (distinguir de homónimas: concatenado coord.)
        # localidad = (self.ciudad+"|"+
        #             str(round(self.latitud,1))+","+
        #             str(round(self.longitud,1))
        # )
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

        return result


class Autom:
    
    '''Automatización de extracción.'''

    estado_des = False

    def __init__(self, extract:Extrac, datalake:DataLake):
        
        self.extract = extract
        self.datalake = datalake
        
    def _bucle_descarga(self, intervalo:int):
        
        '''Método target para el hilo '''

        # Extracción incremental datos de actuales aire y variables meteor.
        while True:
            registro = self.extract.regist_tiempo_df()
            self.datalake.a_parquet_met(
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

    '''Conexión com base OLAP postgreSQL.'''

    def __init__(self) -> None:
        RUTA_CFG = os.path.join(RUTA_DIR, "config.ini")
        config = configparser.ConfigParser()
        config.read(RUTA_CFG)

        bd_cred = config[SQL_DB]
        usu, cont, host = bd_cred["user"], bd_cred["pwd"], bd_cred["host"]
        puet, bd, self.squ = bd_cred["port"], bd_cred["db"], bd_cred["schema"]

        url = f"postgresql://{usu}:{cont}@{host}:{puet}/{bd}?sslmode=require"

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
        y columnas/tipo dato especificado, `si esta NO existe`.

        (!) Primera columna es ID clave primaria y autoincremental 
        por defecto.

        args
            nombre: str nombre de tabla
            cols_type: dict claves=nombre.col / valores=tipo.dato
            id_auto: controla si clave primaria es autoincremental o
            debe proporcionarse.'''
        
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
        except:
            Exception("PostgreSQL error")

    def cargar_df(self, nomb:str, df:pandas.DataFrame):

        '''Cargar dataframe a tabla especificada.

        `Reempleza todos los registros previos.`

        args
            nomb: nombre de la tabla.
            df: dataframe a cargar.'''

        try:
            with self.engine.connect() as con, con.begin():
                print("Conectando con postgreSQL...")
                df.to_sql(
                    name=nomb,
                    con=con,
                    schema=self.squ,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=1000
                )
        except:
            raise Exception("Error de carga: pandas.Dataframe >> postgreSQL")
    
    def impr_tabla(self, nomb:str):

        '''Imprime todos los campos y registros de la tabla
        especificada.'''

        with self.engine.connect() as con:
            df = pandas.read_sql_table(
                table_name=nomb,
                schema=self.squ, 
                con=con)

            print(df)