#Script para contabilizar los viajes totales en los que se ve involucrada cada
#estación en cada día de la semana, a partir de uso_medio_salidas.json y uso_medio_llegadas.json
import os
import json
import pandas as pd
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_251\\'

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import sum as _sum
if __name__ == "__main__":
    with open("uso_medio_llegadas.json", 'r') as fichero:
        frame = json.load(fichero)
        df = pd.DataFrame(frame,columns = ['weekday','id','count'])
    with open("uso_medio_salidas.json", 'r') as fichero:
        frame = json.load(fichero)
        df2 = pd.DataFrame(frame, columns = ['weekday','id','count'])
    df.append(df2, ignore_index=True)
    viajes = spark.createDataFrame(df)
    viajes = viajes.groupby(['weekday','id']).agg(_sum('count')).withColumnRenamed('sum(count)', 'count')
    with open("uso_medio_viajes_totales.json",'w') as fichero:
        json.dump(viajes.collect(),fichero)