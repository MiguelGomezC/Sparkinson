"""
Reúne los resultados obtenidos a partir de frecuencias_habituales.py,
y genera métricas de frecuencia a partir de estas, que se considerarán
representativas del comportamiento habitual de BICIMAD
por estación y día de la semana.
"""
import os
import json
import pandas as pd
from os import listdir
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_251\\'

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

if __name__ == "__main__":
    
    with open('dataset_frecuencias_habituales/resultados/salidas.json','r') as ficherosalidas:
        frames = [json.loads(line) for line in ficherosalidas] #Hay tantas líneas como ficheros se analizaron en frecuencias_habituales.py
    frame = frames.pop()
    for i in frames:
        frame.extend(i)
    df = pd.DataFrame(frame,columns = ['weekday','idunplug_station','count'])
    salidas = spark.createDataFrame(df)
    
    with open('dataset_frecuencias_habituales/resultados/llegadas.json','r') as ficherollegadas:
        frames = [json.loads(line) for line in ficherollegadas]
    frame = frames.pop()
    for i in frames:
        frame.extend(i)
    df = pd.DataFrame(frame,columns = ['weekday','idplug_station','count'])
    llegadas = spark.createDataFrame(df)
    
    #Sumar las apariciones de cada par (weekday, estación) distinto
    llegadas.groupby(['weekday','idplug_station']).agg(sum('count')).withColumnRenamed('sum(count)', 'count')
    salidas.groupby(['weekday','idunplug_station']).agg(sum('count')).withColumnRenamed('sum(count)', 'count')
    