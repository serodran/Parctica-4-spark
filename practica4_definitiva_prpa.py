from pyspark import SparkContext
import json
import sys
import matplotlib
import matplotlib.pyplot as plt


sc = SparkContext()

def mapper_edad(line):
    data = json.loads(line)
    tiempo_viaje = data['travel_time']
    rango_edad = data['ageRange']
    return rango_edad,tiempo_viaje

def mapper_usuario(line):
    data = json.loads(line)
    tiempo_viaje = data['travel_time']
    tipo_usuario = data['user_type']
    return tipo_usuario,tiempo_viaje


def mapper_usuario_unico(line):
    data = json.loads(line)
    codigo_usuario = data['user_day_code']
    return codigo_usuario,1

def mapper_duracion(line):
    data = json.loads(line)
    tiempo_viaje = data['travel_time']
    return '1', tiempo_viaje
    

    
def crear_lista_rdd(lista):
    result1 = []
    result2 = []
    for i in lista:
        result1.append(i[0])
        result2.append(list(i[1]))
    return [result1,result2]

def crear_lista(lista):
    result1 = []
    result2 = []
    for i in lista:
        result1.append(i[0])
        result2.append(i[1])
    return [result1,result2]

#Esta función realiza un estudio de de los tiempos que pasan los usuarios en las bicis en función de sus grupos de edad.
def edad(rdd19, rdd20):
    
	rdd19_edad_media = rdd19.map(mapper_edad).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	ejes19 = crear_lista(list(rdd19_edad_media))
    
	rdd20_edad_media = rdd20.map(mapper_edad).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
	ejes20 = crear_lista(list(rdd20_edad_media))
    
# Representamos la información obtenida en un gráfico de barras
	bar_width = 0.3 #ancho de las barras
    
	for i in ejes20[0]:
		ejes20[0][i] = ejes20[0][i] + bar_width #Pone las graficas una al lado de otra siendo ambas del mismo rango
                                                #para poder compararlas mejor
	for i in range(0,len(ejes19[1])):
		ejes19[1][i] = ejes19[1][i]/60      #El eje y representa el tiempo en segundos dividimos entre 60 para que nos de la suma en minutos
	for i in range(0,len(ejes20[1])):
		ejes20[1][i] = ejes20[1][i]/60
        
	matplotlib.pyplot.bar(ejes19[0],ejes19[1],bar_width,color='g',label='2019')
	matplotlib.pyplot.bar(ejes20[0],ejes20[1],bar_width,color='y',label='2020')
	plt.legend(loc='best')
	plt.title('Tiempos de los usuarios en función de la edad')
	plt.show()



#Funcion para obtener un estudio del numero de usuarios 

def usuario_unico(urdd19, urdd20, meses):
    
    users19 = []
    users20 = []
    
    for i in range(len(meses)):
        
        rdd19_usuario_unico = (urdd19[i]).map(mapper_usuario_unico).groupByKey().collect()
        usuario19 = len((list(rdd19_usuario_unico)))
        users19.append(usuario19)
        
        
        rdd20_usuario_unico = (urdd20[i]).map(mapper_usuario_unico).groupByKey().collect()
        usuario20 = len((list(rdd20_usuario_unico)))
        users20.append(usuario20)
        
# Representamos la información obtenida en un gráfico de barras
    bar_width = 0.3 #ancho de las barras
    
   
    mes2 = []          #Pone las graficas una al lado de otra siendo ambas del mismo mes
    for i in meses:   #para poder compararlas mejor
        mes2.append(i+bar_width)
        
    matplotlib.pyplot.bar(meses,users19,bar_width,color='g',label='2019')
    matplotlib.pyplot.bar(mes2,users20,bar_width,color='y',label='2020')
    plt.legend(loc='best')
    plt.title('Número de usuarios al mes')
    plt.show()
    

#Calcula el tiempo medio de viaje de los usuarios
def duracion(rdd19,rdd20):
    rdd19_duracion = rdd19.map(mapper_duracion).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
    rdd20_duracion = rdd20.map(mapper_duracion).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
    print('Mostramos la duración media del trayecto en 2019 y 2020')
    print(int(rdd19_duracion[0][1]//60))
    print(int(rdd20_duracion[0][1]//60))



def proceso(rdd19,rdd20,urdd19,urdd20, meses):  #Esta función ejcuta las 3 funciones que hemos utilizado para relizar el estudio
    edad(rdd19, rdd20)
    usuario_unico(urdd19, urdd20, meses) 
    duracion(rdd19,rdd20)

def main(sc, años, meses):
    rdd19 = sc.parallelize([])
    rdd20 = sc.parallelize([])
    urdd19 = []
    urdd20 = []
    for y in años:
        for m in meses:
            if y == 2020 or (y == 2019 and m > 6):
                if m<10:
                    filename = f"{y}0{m}_movements.json"
                else:
                    filename = f"{y}{m}_movements.json"
            else:
                filename = f"{y}0{m}_Usage_Bicimad.json"
            if y == 2019:
                rdd19=rdd19.union(sc.textFile(filename))
                urdd19.append(sc.textFile(filename))
            else:
                rdd20 = rdd20.union(sc.textFile(filename))
                urdd20.append(sc.textFile(filename))
    proceso(rdd19,rdd20,urdd19,urdd20, meses)
    

if __name__ =="__main__":
	if len(sys.argv) <= 1:
		años=[2019,2020] 
	else:
		años=list(map(int, sys.argv[1][1:-1].split(",")))
	if len(sys.argv) <= 2:
		meses=[5,6,7,8,9,10] # El estudio lo hacemos de 6 meses ( de mayo a octubre) en 2019 y 2020
		
	else:
		meses=list(map(int, sys.argv[2][1:-1].split(",")))

	main(sc, años,meses)