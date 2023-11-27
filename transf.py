'''
### Tercera entrega: Curso Data Engineer - UTN ###
Modulo 2: Transformación de datos

Alumno: Molina Gabriel
'''
import pandas as pd
import numpy as np

from main import PgSql, DataLake


if __name__ == "__main__":
    print("\nCurso Data Engineer - UTN\n## Procesado de datos ##\n")
    
    ##### Lectura de Data Lake #####
    print("\nLectura desde Data Lake...\n")    
    try:
        df_meteor = DataLake.leer_parq("regmeteor")
    except:
        raise Exception("Error al cargar archivos .parquet desde: 'regmeteor'")
    try:
        df_locs = DataLake.leer_parq("localid")
    except:
        raise Exception("Error al cargar archivos .parquet desde: 'localid'")
    
    print(
        "\n#### #### Lectura de registros meteorológicos desde 'regmeteor' #### ####\n\n",
        df_meteor,
        "\n#### #### Lectura de registros meteorológicos desde 'localid' #### ####\n\n",
        df_locs
    )
    
    ##### Procesado #####

    ### Datos meteorológicos
    
    # Eliminar columna de partición 
    df_meteor = df_meteor.drop(columns=["fecha_partic"])
    
    # Hora de GMT-0 a GMT-3 (hora local Argentina)
    df_meteor.time = pd.to_datetime(df_meteor.time)
    df_meteor.time = df_meteor.time - pd.Timedelta(hours=3)

    # Formateo  y separación en fecha y hora (str / object)
    df_meteor["date"] = df_meteor["time"].dt.strftime("%d/%m/%Y")
    df_meteor["time"] = df_meteor["time"].dt.strftime("%H:%M")

    # "winddirection_10m" (en grados N=0, sentido horario) a puntos 
    #   cardinales (N, S, E, W)
    def a_cardinales(grados:int):
        
        '''Transforma dirección del viento en grados (°) a 
        puntos cardinales (Eng).
        
        args
            `grados` : valor en grados a transformar.'''
        
        if grados == 0 or grados == 360:
            return "N"
        elif grados > 0 and grados < 90:
            return "NO"
        elif grados == 90:
            return "W"
        elif grados > 90 and grados < 180:
            return "SO"
        elif grados == 180:
            return "S"
        elif grados >  180 and grados < 270:
            return "SE"
        elif grados == 270:
            return "E"
        else:
            return "NE"
    
    df_meteor["winddir_cardinal_10m"] = df_meteor["winddirection_10m"].apply(
        lambda g: a_cardinales(g)
        )
    
    # transformar "pressure_msl" de hPa a mmHg
    F_CONV = 0.750064 #hPa a mmHg
    df_meteor["pressure_msl"] = df_meteor["pressure_msl"]*F_CONV

    # Pasar "is_day" a bool
    df_meteor['is_day'] = df_meteor['is_day'].apply(
        lambda a: a == 1 if True else False
        )    

    # Traer ciudad y país desde tabla localidades
    tb_salida = df_meteor.merge(
        df_locs[["id","name","country"]],
        how="left",
        left_on="api_loc_id",
        right_on="id"
    )
    tb_salida.drop(columns=["id"],inplace=True)
    tb_salida.rename(columns={"name":"city"}, inplace=True)

    # Reordenar columnas
    tb_salida = tb_salida[['date', 'time','city','country',
        'api_loc_id', 'interval', 'temperature_2m', 
        'apparent_temperature', 'relativehumidity_2m',
        'is_day','precipitation', 'rain', 'pressure_msl', 'windspeed_10m',
       'winddir_cardinal_10m','winddirection_10m', 'windgusts_10m']]


    ### Datos localidad

    # Castear a str y formatear 'postcodes'
    df_locs["postcodes"] = df_locs["postcodes"].astype(str)
    df_locs["postcodes"] = df_locs["postcodes"].str.replace("[\"", "")
    df_locs["postcodes"] = df_locs["postcodes"].str.replace("\"]", "")
    df_locs['postcodes'].replace('None', np.nan, inplace=True)

    # Reemplazar None por nan en 'admin3'
    df_locs['admin3'].replace('None', np.nan, inplace=True)


    ##### cargar a Data WareHouse #####

    d_warehouse = PgSql()
    TABLA_MET = "meteor_proc"
    TABLA_LOC = "loc_proc"

    d_warehouse.crear_tb(
          nomb=TABLA_LOC,
          cols_type={
                'ID': "INT PRIMARY KEY NOT NULL",
                'name':"CHAR",
                'latitude':"FLOAT",
                'longitude':"FLOAT",
                'elevation':"FLOAT",
                'feature_code':"CHAR",
                'country_code':"CHAR",
                'admin1_id':"FLOAT",
                'admin2_id':"FLOAT",
                'timezone':"CHAR",
                'population':"INT",
                'country_id':"INT",
                'country':"CHAR",
                'admin1':"CHAR",
                'admin2':"CHAR",
                'postcodes':"INT",
                'admin3_id':"INT",
                'admin3':"CHAR"
          },
          id_auto=False
    )
    d_warehouse.cargar_df(TABLA_LOC, df_locs)
    print(f"\nVERIFICAR:\n-Impresión desde base de datos-\n'{TABLA_LOC}'\n")
    d_warehouse.impr_tabla(TABLA_LOC)

    # Datos meteorológicos ()
    d_warehouse.crear_tb(
        nomb=TABLA_MET,
        cols_type={
            "date" :                    "DATE",
            "time" :                    "TIME",
            "city" :                     "CHAR",
            "country"      :            "CHAR",
            "api_loc_id":                "INT",
            "interval":                  "INT",
            "temperature_2m":            "FLOAT",
            "relativehumidity_2m":       "FLOAT",
            "apparent_temperature":      "FLOAT",
            "is_day":                    "BOOL",
            "precipitation":             "FLOAT",
            "rain" :                     "FLOAT",
            "pressure_msl" :             "FLOAT",
            "windspeed_10m" :            "FLOAT",
            "winddir_cardinal_10m" :     "CHAR",
            "winddirection_10m"  :       "FLOAT",
            "windgusts_10m" :            "FLOAT"
        })
    d_warehouse.cargar_df(TABLA_MET, tb_salida)

    print(f"\nVERIFICAR:\n-Impresión desde base de datos-\n'{TABLA_MET}'\n")
    d_warehouse.impr_tabla(TABLA_MET)

