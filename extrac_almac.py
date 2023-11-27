'''
### Tercera entrega: Curso Data Engineer - UTN ###

    - Módulo con ejecución de pasos de Extracción y Almacenamiento (Data Lake) - 

Alumno: Molina Gabriel
'''

import pandas # (usado para formatear impresión)

from main import DataLake, Extrac, Autom 


if __name__ == "__main__":
    print("\nCurso Data Engineer - UTN\n## Extracción de datos ##\n")
    localid = "La Plata"
    
    print(f"Extracción de información geografica (full)\n EJ: {localid}")
    
    loc = Extrac.ciudad_df(localid, nres=10)
    print(loc)
    loc_id = loc["id"]
    
    # Guarda búsquedas por nombre
    datalake = DataLake()    
    datalake.a_parquet(loc)
    
    print("\n***** ***** ***** ***** ***** *****\n")
    print("--- Info: localidad elegida ---\n")

    # (solo para comodidad de impresión)
    print(pandas.DataFrame({
        "INFO":loc.columns,
        "VALOR":loc.iloc[0].to_list()}
        ))
    
    print(f"\nExtracción de información meteorológica actual \
(incremental)\n EJ: {localid}\n")

    extraccion = Extrac(loc_id, latitud=-34.92145,longitud=-57.95453)
    
    automat = input("Para ejecutar muestra de extracción \
automatizada, ingresar: a \nPara ejecutar solo una iteración de \
extracción, ingresar: <cualquier otra tecla> \n\n\n>: ")

    if automat == "a":
        print("\nEXTRACCIÓN AUTOMATICA\n")
        control = Autom(extraccion, datalake)
        control.lanzar_descarga(intervalo=900)
    else:
        print("\nEXTRACCIÓN registro tiempo actual\n")
        registro = extraccion.regist_tiempo_df()
        print("\nREGISTRO DESCARGADO:\n",
            "- Hora convertida de GMT+0 a GMT-3 (local, ARG) -\n", 
            registro
        )
        print("\n******\nRegistros previos:\n", datalake.leer_parq("regmeteor"),
            "\n******")
        datalake.a_parquet_met(registro=registro, partic=["fecha_partic"])
