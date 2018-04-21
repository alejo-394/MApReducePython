'''
Función: utilice MapReduce de Python puro para buscar archivos de gran tamaño
 y tome las primeras 20 palabras por frecuencia:

Obs: El tiempo total de finalización de tareas es igual al tiempo de proceso 
de la última tarea completada. 

'''

import string

from SimpleMapReduced import SimpleMapReduce

#Esta Función cumple la función de Mapeo
def file_to_words(filename):
    """
        Lee un archivo y regrese una secuencia de valores (palabra, ocurrencias).
    """
    #Evite los artículos, preposiciones que evitan que palabras relevantes surjan
    STOP_WORDS = set(['de', 'el', 'y', 'que', 'de', 'a', 'y', 'no', 'es',
                      'con', 'en', 'por'])

    # Crear una tabla de asignación y reemplazar la puntuación con espacios
    TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))   

    # Imprimir el nombre actual del proceso del trabajador
    output = []

    with open(filename, 'rt') as f:
        for line in f:
            line = line.translate(TR) 
            for word in line.split():
                word = word.lower()
                if word.isalpha() and word not in STOP_WORDS:   
                    output.append( (word, 1) )
    return output

#Esta función cumple el papel de reducción
def count_words(item):
    """
        Convierta los datos particionados para una palabra a
        tupla que contiene la palabra y el número de ocurrencias.
    """
    word, occurances = item
    return (word, sum(occurances))


if __name__ == '__main__':
    import operator
    import glob # Devuelve la lista de rutas de archivos

    input_files = glob.glob('*.txt')   #se revisará un comjunto de archivos
    print(input_files)
        
    mapper = SimpleMapReduce(file_to_words, count_words,15)
    word_counts = mapper(input_files)   #Llamar __call__
    word_counts.sort(key=operator.itemgetter(1))
    word_counts.reverse()
    
    print('\nPrimeras 20 Palabras por Frecuencia\n')
    top20 = word_counts[:20] if len(word_counts)>=20 else word_counts
    longest = max(len(word) for word, count in top20)
    for word, count in top20:
        print( '%-*s: %5s' % (longest+1, word, count))
