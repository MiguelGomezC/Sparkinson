"""
Re√∫ne los resultados obtenidos a partir de frecuencias_habituales.py,
y genera m√©tricas de frecuencia a partir de estas, que se considerar√°n
representativas del comportamiento habitual de BICIMAD
por estaci√≥n y d√≠a de la semana.
"""
import os
import json
import pandas as pd
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_251\\'
import collections, functools, operator

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.types import *
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.functions import sum as _sum

if __name__ == "__main__":
    
    with open('dataset_frecuencias_habituales/resultados/salidas.json','r') as ficherosalidas:
        frames = [json.loads(line) for line in ficherosalidas] #Hay tantas l√≠neas como ficheros se analizaron en frecuencias_habituales.py
    frame = frames.pop()
    for i in frames:
        frame.extend(i)
    df = pd.DataFrame(frame,columns = ['weekday','idunplug_station','count'])
    global salidas
    salidas = spark.createDataFrame(df)
    
    with open('dataset_frecuencias_habituales/resultados/llegadas.json','r') as ficherollegadas:
        frames = [json.loads(line) for line in ficherollegadas]
    frame = frames.pop()
    for i in frames:
        frame.extend(i)
    df = pd.DataFrame(frame,columns = ['weekday','idplug_station','count'])
    llegadas = spark.createDataFrame(df)
    
    with open('dataset_frecuencias_habituales/resultados/diccionarios.json','r') as ficherodiccionarios:
        diccionarios = [json.loads(line) for line in ficherodiccionarios]
    diccionario = dict(functools.reduce(operator.add, map(collections.Counter, diccionarios)))
    
    print('N˙mero total de viajes analizados = ', diccionario['viajes totales'], '\n')
    
    #Sumar las apariciones de cada par (weekday, estaci√≥n) distinto
    salidas = salidas.groupby(['weekday','idunplug_station']).agg(_sum('count')).withColumnRenamed('sum(count)', 'count')
    llegadas = llegadas.groupby(['weekday','idplug_station']).agg(_sum('count')).withColumnRenamed('sum(count)', 'count')
    
    #Calcular media de la cantidad de viajes por dÌa de la semanas
    def average(count, weekday):
        return int(count)/diccionario[str(weekday)]

    average_pwd = udf(average, DoubleType()) #Considerar cambiar a divisiÛn entera para reducir tiempo de ejecuciÛn

    salidas = salidas.select('idunplug_station', 'weekday', average_pwd('count','weekday'))
    with open('uso_medio_salidas.json','w') as salidasfinal:
        json.dump(salidas.collect(), salidasfinal)
    llegadas = llegadas.select('idplug_station', 'weekday', average_pwd('count','weekday'))
    with open('uso_medio_llegadas.json','w') as llegadasfinal:
        json.dump(llegadas.collect(), llegadasfinal)
    
    
    