"""
Descargar en el directorio de este archivo todos los datasets que se deseen
usar para calcular las métricas de frecuencia habituales.
A partir de estas, asumir que las medidas que se obtienen son representativas
del comportamiento usual de BICIMAD con cada estación.
"""

import os
import json
from os import listdir
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_251\\'

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from datetime import date

def get_weekday(hourtime):
    """
    Devuelve el día de la semana a partir de un iterador que contiene
    una fecha con un entero
    0:Lunes,...,6:Domingo
    """
    date_str = (list(hourtime)[0])
    year = int(date_str[:4])
    month = int(date_str[5:7])
    day = int(date_str[8:10])
    return date(year,month,day).weekday()

from pyspark.sql.types import *
from pyspark.sql.functions import col, udf, pandas_udf, PandasUDFType
day_func = udf(get_weekday, IntegerType())

def quantities(path):
    """
    Dado el nombre de un archivo en el directorio de datasets,
    escribe dos json en la carpeta resultados (de ese mismo directorio):
        Uno con la cantidad de salidas por estación y día de la semana
        Otro con la cantidad de llegadas por estación y día de la semana
    Si el archivo que escribe ya existía previamente, añade nueva información una línea más abajo
    (formato un json por línea)
    """
    df = spark.read.option("mode", "DROPMALFORMED").json(path) #El modo DROPMALFORMED ignora las líneas que no se pudo parsear correctamente
    salidas = df.select(day_func('unplug_hourTime'),'idunplug_station').withColumnRenamed('get_weekday(unplug_hourTime)', 'weekday')
    salidas = salidas.groupby(['weekday','idunplug_station']).count()
    with open('resultados/salidas.json','a') as ficherosalidas:
        json.dump(salidas.collect(), ficherosalidas)
    llegadas = df.select(day_func('unplug_hourTime'),'idplug_station').withColumnRenamed('get_weekday(unplug_hourTime)', 'weekday')
    llegadas = llegadas.groupby(['weekday','idplug_station']).count()
    with open('resultados/llegadas.json','a') as ficherollegadas:
        json.dump(llegadas.collect(), ficherollegadas)
    #Para contar la cantidad de viajes que había en path, hacer list(llegadas.groupBy().agg(sum("count")).collect())[0]
    #Es mucho más fácil, sin embargo, hacer df.count() y tiene un orden de complejidad menor

if __name__ == "__main__":
    files = listdir('C:\\Users\Miguel\Documents\GitHub\Sparkinson\dataset_frecuencias_habituales')
    for name in files:
        if name[-4:]=="json":
            quantities(name)