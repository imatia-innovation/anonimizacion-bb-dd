# -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 10:38:58 2024

@author: olivia.castineiras
"""

import random
import pandas as pd  # Importar pandas
import logging as log

# Función para crear la matriz de permutación inicial
def crear_matriz_permutacion(num_filas, num_columnas):
    # Crea una matriz de permutación vacía (sin la primera fila)
    return [[None] * num_columnas for _ in range(num_filas - 1)]

# Función para mezclar los índices de una columna específica
def mezclar_indices_columna(permutacion, num_filas, col, max_intentos=5):
    indices_disponibles = list(range(1, num_filas))  # Omite la primera fila
    print(f"\n--- Procesando columna {col} ---")
    print("Índices disponibles al inicio:", indices_disponibles)
    
    for fila in range(1, num_filas):
        intentos = 0
        asignado = False
        
        while intentos < max_intentos and indices_disponibles:
            indice_aleatorio = random.choice(indices_disponibles)
            intentos += 1
            
            # Verifica que no se haya usado previamente en la fila
            if all(permutacion[fila-1][prev_col] != indice_aleatorio for prev_col in range(col)):
                permutacion[fila-1][col] = indice_aleatorio
                indices_disponibles.remove(indice_aleatorio)
                asignado = True
                print(f"  Índice {indice_aleatorio} asignado a fila {fila}, columna {col}")
                print(f"  Índices disponibles actualizados: {indices_disponibles}")
                break
        
        if not asignado:
            # Si no se pudo asignar, lo asigna aunque coincida
            indice_aleatorio = random.choice(indices_disponibles)
            permutacion[fila-1][col] = indice_aleatorio
            indices_disponibles.remove(indice_aleatorio)
            print(f"  No se encontró un valor único después de {max_intentos} intentos.")
            print(f"  Índice {indice_aleatorio} asignado a fila {fila}, columna {col} aunque coincida.")
            print(f"  Índices disponibles actualizados: {indices_disponibles}")
        
        print(permutacion)
    
    return permutacion

# Función para agregar la fila de encabezados y organizar la matriz final
def agregar_encabezados_y_organizar(matriz, permutacion, columnas_a_mezclar):
    num_filas = len(matriz)
    num_columnas = len(matriz[0])
    
    matriz_permutada_indices = [matriz[0]]  # Agregar encabezados originales
    for fila in range(num_filas - 1):
        nueva_fila = [None] * num_columnas
        for col in range(num_columnas):
            if col in columnas_a_mezclar:
                nueva_fila[col] = permutacion[fila][col]  # Asigna índices permutados
            else:
                nueva_fila[col] = None  # Deja None en columnas no mezcladas
        matriz_permutada_indices.append(nueva_fila)
    print(matriz_permutada_indices)
    
    return matriz_permutada_indices

# Función principal que llama al resto de funciones del codigo
def permutar_filas(matriz, columnas_a_mezclar, max_intentos=5):
    num_filas = len(matriz)
    num_columnas = len(matriz[0])
    
    # Crea la matriz de permutación inicial
    permutacion = crear_matriz_permutacion(num_filas, num_columnas)
    
    # Itera sobre las columnas especificadas y mezcla los índices
    for col in columnas_a_mezclar:
        permutacion = mezclar_indices_columna(permutacion, num_filas, col, max_intentos)
    
    # Organiza la matriz final con encabezados y las columnas mezcladas
    matriz_permutada_indices = agregar_encabezados_y_organizar(matriz, permutacion, columnas_a_mezclar)
    
    return matriz_permutada_indices

# Función para guardar la matriz en un archivo CSV
def guardar_en_csv(matriz_permutada_indices, archivo_salida):
    pd.DataFrame(matriz_permutada_indices).to_csv(archivo_salida, index=False, header=False)
    print(f"\nLa matriz de índices permutados se ha guardado en {archivo_salida}.")

# Función main que coordina la lectura, permutación y guardado
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
        guardar_en_csv(matriz_permutada_indices, archivo_salida)

    except ValueError as e:
        print(e)

if __name__ == "__main__":
    main()
