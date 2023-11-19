%md
# Examen Diagnostico Data Engineer Jr
**cuentas con 24 horas para resolver el ejercicio**

## Instrucciones

1. Debes crear un repositorio de github en dónde entregarás tu notebook
2. Crear una rama que por nombre lleve tus iniciales.
3. Es importante que el primer commit se haga con tres archivos:
    * _README.md_ contendrá estas instrucciones
    * _solution.ipynb_ contendrá la exportación de este notebook como ipynb
    * _players_21.csv_ será el archivo de datos csv tomado desde: https://raw.githubusercontent.com/dagarciam/diagnostico_pyspark/solution/resources/data/players_21.csv
4. El ejercicio debe ser resuelto en un notebook de Databricks Community
    * Aquí puedes encontrar como crear tu cuenta gratuita: https://docs.databricks.com/en/getting-started/community-edition.html
6. Agregué una sección a este README.md 
    * Documente el algoritmo implementado para resolver el **Ejercicio 1** utilizando un ejemplo y mostrando paso a paso como se encuentra la solución.
    * Con las instrucciones de uso de la función x| y un ejemplo de uso.

_**Haz que tu repositorio sea privado** y brinda acceso de lectrua a tu repositorio al usuario **(indicado en el correo)**_

## ¿Qué evaluaremos?

* El Ejercicio 1 y 2 debe realizarse sin librerias
* Resuelva el Ejercicio 3 con el API de SparkSQL el ejercicio planteado.
* El uso de sentencias SQL queda estrictamente prohibido.
* El uso de cadenas en las clases que implementan la lógica de solución están muy mal vistos por nuestra area de QA, sea
  cuidadoso.
* Modularice sú código lo suficiente de tal forma que cada función haga una sola cosa.

## Ejercicio 1

1. Desarrolla un método que dada una lista de números enteros encuentre los val
ores que sumados den como resultado un valor objetivo.
  * El método recibirá los siguientes parámetros:
    1. Lista con valores enteros
    2. Valor objetivo
  * El método deberá de retornar una lisla con los valores obtenidos.
  * Para obtener la combinación de números a retornar sólo será posible utilizar una vez cada elemento de la lista enviada como parámetro (la lista puede contener valores repetidos)
##### Ejemplo 
```
my_list = [1, 2, 5, 3]
target = 6
result = my_function(my_list, target)
```
##### Resultado 
`result = [[1, 5], [1, 2, 3]]`
Porque ambas combinaciones suman 6.

## Ejercicio 2

Desarrolla un programa que dadas dos listas A y B genere una lista C que contendrá en la i-ésima posición el conteo de elementos de A que son menores a la i-ésima posición de B.
 
##### Ejemplo
```
A = [1, 6, 3, 8, 1, 3]
B = [6, 2, 1, 9]

C = my_function(A,B)
```
##### Resultado

```
C = [4, 2, 0, 6]
```

##### Explicación:
* _La primer posición de C es 4 ya que sólo existen 4 números de la lista A menores al valor 6 de la primer posición de B_
* _La segunda posición de C es 2 ya que sólo existen 2 números de la lista A menores al valor 2 de la segunda posición de B_
* _La tercer posición de C es 0 ya que no existen números de la lista A menores al valor 1 de la tercer posición de B_
* _La cuarta posición de C es 6 ya que existen 6 números de la lista A menores al valor 9 de la cuarta posición de B_

## Ejercicio 3
1. La tabla de salida debe contener las siguientes columnas:
   `short_name, long_name, age, height_cm, weight_kg, nationality, club_name, overall, potential, team_position`
2. Agregar una columna `player_cat` que responderá a la siguiente regla (rank over Window particionada por `nationality` y `team_position`
   y ordenada por `overall`):
    * **A** si el jugador es de los mejores 3 jugadores en su posición de su país.
    * **B** si el jugador es de los mejores 5 jugadores en su posición de su país.
    * **C** si el jugador es de los mejores 10 jugadores en su posición de su país.
    * **D** para el resto de jugadores.

   ***tip** para resolver este ejercicio, este blog puede ayudar: https://sparkbyexamples.com/pyspark/pyspark-window-functions
3. Agregaremos una columna `potential_vs_overall` con la siguiente regla:
    * Columna `potential` dividida por la columna `overall`
4. Filtraremos de acuerdo a las columnas `player_cat` y `potential_vs_overall` con las siguientes condiciones:
    * Si `player_cat` esta en los siguientes valores: **A**, **B**
    * Si `player_cat` es **C** y `potential_vs_overall` es superior a **1.15**
    * Si `player_cat` es **D** y `potential_vs_overall` es superior a **1.25**
5. Por favor escriba la tabla resultante de los pasos anteriores particionada por la columna `nationality`, la salida
   debe estar escrita en formato **parquet** y debe usarse el método `coalese(1)`
   para obtener solo un archivo por partición.
6. Agregar el método `run_process` que reciba cuatro parametros 
    * str con el path de donde se van a leer los datos origen
    * str con el path donde se van a escribir los datos de salida
    * int que de ser 1 realizará todos los pasos únicamente para los jugadores menores de 23 años y en caso de ser 0 lo hará con todos los jugadores del dataset
    * `nationality_filter` que de ser `None` no realizará cambios a la salida y de ser cualquier otro valor filtrará la salida con la siguiente condición `col('nationality') == nationality_filter` antes de escribirla.

¡Buena suerte!
EJERCICIO 1
PARA EL EJERCICIO UNO SE USO UN ALGORITMO DE 3 FOR ANIDADOS, ESTO POR PRACTICIDAD YA QUE SON LISTAS CORTAS, EN CASO DE SER LISTAS LARGAS SU BUSCARIA OCUPAR OTRO METODO YA QUE ESTO REQUIERE MUCHOS RECURSOS COMPUTACIONALES. DESPUES DE CREADOS LOS TRES CICLOS FOR ANIDADOS, SE CREA UNA CONDICIONAL QUE SI LOS TRES ELEMENTOS DAN EL RESULTADO OBETIVO PASAN AL SIGUEINTE LINEA, LA SIGUIENTE LINEA (UN IF ANIDADO) ES QUE SI LOS TRES ELEMENTOS SON IGUALES PASE SI NO SE AGREGUE A LA LISTA VACIA. 
PARA UNA DATA MAS LIMPIA, SE CREO UN FILTRO DONDE COMBERTIMOS LAS LISTAS DENTRO DE LA LISTA EN TUPLAS PARA DESPUES LIMPIAR LOS DUPLICADOS PARA AL FINAL VOLVERLA UNA LISTA DE NUEVO. TODO ESTRO DENTRO DE UNA FUNCION QUE NOS RETORNARA LA LISTA VACIA AL MANDARLA LLAMAR LA FUNCION. 

lista = [2, 3, 5, 7, 9] ## DATA INPUT 
objetivo = 15## DATA INPIT 

def my_funcion(lista,obetivo):## CREO UNA FUNCION 
    lista_2 = [] ## LISTA VACIA
    for a in lista: ##PRIMER CICLO FOR
        for b in lista: ## SEGUNDO CICLO FOR
            for c in lista: ## TERCER CICLO FOR 
                if a + b + c == objetivo: #PROMERA CONDICONAL 
                    if a == b == c: ## SEGUNDA COCNDICIONAL
                        pass
                    else:
                        results = [a, b, c]
                        lista_2.append(results)

    lista_2 = list(set(tuple(i) for i in lista_2))  # Convierte a tuplas y luego a set para eliminar duplicados## FILTROS
    lista_2 = [list(i) for i in lista_2]
    print(lista_2)

quesito = my_funcion(lista,objetivo) #MANDAR LLAMAR LA LISTA CON LOS DATOS DE ENTRADA 

print(quesito)

Para este ejercicio se crea una funcion donde primero tenemos un contador en ceros de la misma longitud de la lista a comparar, despues utiliza el ciclo for con el funcion enumerate, donde la primera variable iteradora enumera los elemento de la lista y la segunda toma forma de los elementos de la antes dicha. despues tenmos un ciclo for anidado donde la vartiable iteradota toma forma de los elemento de la lista a. si cumple la condicionale el contador aumentara asi sabiendo cuantos valores menores son menores del elemento de la lista.

def contar_menores(lista_a, lista_b):
    contador = [0] * len(lista_b)  # Inicializa la lista contador con ceros

    for i, valor_b in enumerate(lista_b):
        for valor_a in lista_a:
            if valor_a < valor_b:
                contador[i] += 1
    print(contador)

    return contador

resultado = contar_menores(lista_a, lista_b)
print("Resultado", resultado)

lista_a = [2, 4, 6, 8, 10]
lista_b = [5, 7, 3, 9, 12]