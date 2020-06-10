import os
import json
import pandas as pd
os.environ['JAVA_HOME'] = 'C:\\Program Files\\Java\\jdk1.8.0_251\\'

import json
from pyspark import SparkContext


def mapper_zone(line, zone):
    """
    zone(string) = 'Distrito' o 'Barrio', dependiendo de la división que se quiera hacer
    """
    if zone == 'Barrio':
        liminf = 6
    elif zone == 'Distrito':
        liminf = 4
    estacion=line['Número']
    distrito=line[zone][liminf:]
    return estacion, distrito

def division(zone, sc):
    """
    zone(string) = 'Distrito' o 'Barrio', dependiendo de la división que se quiera hacer
    """
    rdd=sc.parallelize(estaciones)
    mapper = lambda line : mapper_zone(line,zone)
    Dict = rdd.map(mapper).collectAsMap()
    with open('dict'+zone+'.json', 'w') as diccionario:
        json.dump(Dict,diccionario)

if __name__ == "__main__":
    sc = SparkContext()
    with open('distritos_estaciones.json', 'r',encoding="utf8") as f:
        global estaciones
        estaciones = [json.loads(line) for line in f]
    estaciones = estaciones[0][0]
    division('Barrio',sc)
    division('Distrito',sc)
    sc.stop()