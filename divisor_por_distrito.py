
def estacion_distrito(nestacion):
    import json
    from pyspark import SparkContext
    sc = SparkContext()
    def mapper(lines):
        data=json.loads(lines)
        estacion=data['Número']
        distrito=data['Distrito']
        return estacion,distrito
    estaciones=[]
    with open('distritos_estaciones.json', 'r') as f:
      for lines in f:
          estaciones.append(json.loads(lines))
    estaciones = estaciones.pop().pop()
    i = 0
    estjs=[]
    while i < len(estaciones):
        estjs.append(json.dumps(estaciones[i]))
        i += 1
    rdd=sc.parallelize(estjs)
    distrito=rdd.map(mapper).filter(lambda x:x[0]==nestacion).collect()[0][1][4:13]
    return distrito