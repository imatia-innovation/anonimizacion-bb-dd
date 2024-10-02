# -*- coding: utf-8 -*-
"""
Created on Mon Sep 30 12:35:22 2024

@author: olivia.castineiras
"""

import random
import pandas as pd  # Importar pandas
import logging as log

def permutar_filas(matriz, columnas_a_mezclar, max_intentos=5):
    # Obtener las dimensiones de la tabla
    num_filas = len(matriz)
    num_columnas = len(matriz[0])
    
    # Crear una matriz de permutación vacía para almacenar los índices
    permutacion = [[None] * num_columnas for _ in range(num_filas - 1)]  # Solo para las filas a mezclar (sin la primera)
    print('Matriz de permutación inicial:', permutacion)
    
    # Iteramos sobre las columnas especificadas
    for col in columnas_a_mezclar:
        indices_disponibles = list(range(1, num_filas))  # Lista de índices disponibles para esta columna, omitiendo la fila 0
        print(f"\n--- Procesando columna {col} ---")
        print("Índices disponibles al inicio:", indices_disponibles)
        
        # Iteramos por cada fila (omitiendo la primera fila)
        for fila in range(1, num_filas):
            # print(f"\nFila {fila} (columna {col}):")
            
            intentos = 0
            asignado = False
            
            while intentos < max_intentos and indices_disponibles:
                # Seleccionar un índice aleatorio de los disponibles
                indice_aleatorio = random.choice(indices_disponibles)
                intentos += 1
                
                # Verificar que el índice no fue utilizado previamente en esta fila
                if all(permutacion[fila-1][prev_col] != indice_aleatorio for prev_col in range(col)):
                    permutacion[fila-1][col] = indice_aleatorio
                    indices_disponibles.remove(indice_aleatorio)  # Eliminar el índice usado
                    asignado = True
                    print(f"  Índice {indice_aleatorio} asignado a fila {fila}, columna {col}")
                    print(f"  Índices disponibles actualizados: {indices_disponibles}")
                    break
            
            if not asignado:
                # Si no se pudo encontrar un valor que no coincida después de varios intentos,
                # asignar el valor aunque coincida.
                indice_aleatorio = random.choice(indices_disponibles)
                permutacion[fila-1][col] = indice_aleatorio
                indices_disponibles.remove(indice_aleatorio)
                print(f"  No se encontró un valor único después de {max_intentos} intentos.")
                print(f"  Índice {indice_aleatorio} asignado a fila {fila}, columna {col} aunque coincida.")
                print(f"  Índices disponibles actualizados: {indices_disponibles}")
    
        print("indices_mezclados", permutacion)
    # Devolver solo la matriz de permutación (con índices)
    # print("\nMatriz de índices de permutación final:", permutacion)
    
    # Agregar la fila de encabezados de las columnas que no se mezclaron
    matriz_permutada_indices = [matriz[0]]  # Mantener la fila de encabezados original
    for fila in range(num_filas - 1):
        nueva_fila = [None] * num_columnas
        for col in range(num_columnas):
            if col in columnas_a_mezclar:
                nueva_fila[col] = permutacion[fila][col]  # Solo agregamos los índices permutados
            else:
                nueva_fila[col] = None  # Para columnas no mezcladas, dejamos None 
        matriz_permutada_indices.append(nueva_fila)
        
    
    return matriz_permutada_indices


def main():
    # Leer la matriz desde un archivo CSV
    archivo_csv = 'cor_credit_cards.csv'  
    matriz_original = pd.read_csv(archivo_csv, header=None).values.tolist()  # Lee el CSV y lo convierte en una lista

    log.basicConfig(level=log.DEBUG)
    # Definir las columnas a mezclar 
    columnas_a_mezclar = [0, 1]  

    try:
        matriz_permutada_indices = permutar_filas(matriz_original, columnas_a_mezclar)

        # Guardar la matriz de índices permutados en un nuevo archivo CSV
        archivo_salida = 'matriz_permutada_indices.csv'  # Cambia esto a la ruta donde quieras guardar el CSV
        pd.DataFrame(matriz_permutada_indices).to_csv(archivo_salida, index=False, header=False)
        print(f"\nLa matriz de índices permutados se ha guardado en {archivo_salida}.")
    except ValueError as e:
        print(e)

if __name__ == "__main__":
    main()
