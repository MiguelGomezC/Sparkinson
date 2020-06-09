
def estacion_distrito(nestacion):
    import json
    from pyspark import SparkContext
    sc = SparkContext()
    def mapper(line):
        estacion=line['Número']
        distrito=line['Distrito']
        return estacion,distrito
    estaciones=[]
    with open('distritos_estaciones.json', 'r',encoding="utf8") as f:
      for lines in f:
          estaciones.append(json.loads(lines))
    estaciones = estaciones.pop().pop()
    i = 0
    estjs=[]
    while i < len(estaciones):
        estjs.append(estaciones[i])
        i += 1
    rdd=sc.parallelize(estjs)
    distrito=rdd.map(mapper).filter(lambda x:x[0]==nestacion).collect()[0][1][4:13]
    sc.stop()
    return distrito