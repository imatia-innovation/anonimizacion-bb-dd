# -*- coding: utf-8 -*-
"""
Created on Tue Oct  8 12:03:12 2024

@author: olivia.castineiras
"""

import random
import pandas as pd
import logging as log

# Función para crear la matriz de permutación inicial
def crear_matriz_permutacion(num_filas, num_columnas):
    print(f"Creando matriz de permutación inicial con {num_filas} filas y {num_columnas} columnas")
    return [[None] * num_columnas for _ in range(num_filas)]  # Excluye la primera fila (encabezados)

# Función para mezclar los índices de una columna específica
def mezclar_indices_columna(permutacion, num_filas, col, max_intentos=5):
    indices_disponibles = list(range(0,num_filas))  # Omite la primera fila (encabezados)
    print(indices_disponibles)
    for fila in range(0,num_filas):
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
                break
        
        if not asignado:
            # Si no se pudo asignar, lo asigna aunque coincida
            indice_aleatorio = random.choice(indices_disponibles)
            permutacion[fila-1][col] = indice_aleatorio
            indices_disponibles.remove(indice_aleatorio)
    
    return permutacion

# Función para separar la columna "email" si existe
def separar_emails(df):
    if 'email' in df.columns:
        print("Separando la columna 'email' en partes")
        
        # Dividir la columna "email" en nombre y dominio
        df[['anterior', 'domain']] = df['email'].str.split('@', n=1, expand=True)
        df['domain'] = '@' + df['domain'].astype(str)  # Añadir '@' al dominio
        
        # Separar 'name' por "." o "_"
        name_parts = df['anterior'].str.split(r'[._]', expand=True)

        # Renombrar las columnas de name_parts como name_1, name_2, ...
        name_parts.columns = [f'anterior_{i+1}' for i in range(name_parts.shape[1])]

        # Pone las partes en las que dividimos 'name' al final del df 
        df = pd.concat([df.drop(columns=['email', 'anterior']), name_parts], axis=1)  # Eliminar 'name' y 'email'

        # Reorganizar las columnas para que 'domain' sea la última columna
        cols = df.columns.tolist()  # Obtener la lista de columnas
        cols.append(cols.pop(cols.index('domain')))  # Mover 'domain' al final
        df = df[cols]  # Reorganizar el DataFrame
        
        # Devolver el DataFrame y el número de partes del nombre
        num_partes_nombre = name_parts.shape[1]
        print(f"Número de partes del nombre: {num_partes_nombre}")
        return df, num_partes_nombre

    return df, 0  # Si no existe 'email', devuelve el dataframe sin cambios y 0 partes

def unir_emails(df, num_partes_nombre):
    # Generar nombres de columnas para partes del nombre
    nombre_columnas = [f'anterior_{i+1}' for i in range(num_partes_nombre)]
    
    # Comprobar si la columna 'domain' y todas las partes del nombre existen
    if 'domain' in df.columns and all(col in df.columns for col in nombre_columnas):
        print(f"Uniendo las partes de 'email'. Número de partes del nombre: {num_partes_nombre}")

        # Crear una función para unir partes del nombre
        def construir_email(row):
            # Convertir todas las partes a string y unirlas (eliminando cadenas vacías)
            partes = [str(part) for part in row[nombre_columnas] if pd.notna(part)]
            # Unir las partes y agregar el dominio
            return '.'.join(partes) + str(row['domain']) if partes else None

        # Aplicar la función para construir el email
        df['email'] = df.apply(construir_email, axis=1)

        # Eliminar las partes del nombre y el dominio
        df.drop(columns=nombre_columnas + ['domain'], inplace=True)
        
        print("Columna 'email' reconstruida:")
        print(df['email'].head())  # Imprimir los primeros valores de la nueva columna 'email' para verificar
    else:
        missing_columns = [col for col in nombre_columnas if col not in df.columns] + (['domain'] if 'domain' not in df.columns else [])
        print(f"Advertencia: No se pudo unir 'email' porque faltan columnas necesarias: {missing_columns}")

    return df

# Nueva función para separar el nombre en partes y mezclar
def separar_nombre(df):
    if 'name' in df.columns:
        print("Separando la columna 'name' en partes")
        
        # Dividir la columna "name" en tantas partes como sea necesario
        name_parts = df['name'].str.split('.', expand=True)  # Separar por espacio

        # Renombrar las columnas de name_parts como name_1, name_2, ...
        name_parts.columns = [f'nombre_{i+1}' for i in range(name_parts.shape[1])]

        # Pone las partes en las que dividimos 'name' al final del df 
        df = pd.concat([df.drop(columns=['name']), name_parts], axis=1)  # Eliminar 'name'

        # Reorganizar las columnas para que la última sea la más baja
        cols = df.columns.tolist()  # Obtener la lista de columnas
        df = df[cols]  # Reorganizar el DataFrame
        
        # Devolver el DataFrame y el número de partes del nombre
        num_partes_nombre = name_parts.shape[1]
        print(f"Número de partes del nombre: {num_partes_nombre}")
        return df, num_partes_nombre

    return df, 0  # Si no existe 'name', devuelve el dataframe sin cambios y 0 partes

def unir_nombres(df):
    # Generar nombres de columnas para partes del nombre
    nombre_columnas = [col for col in df.columns if col.startswith('nombre_')]
    
    # Comprobar si existen partes del nombre
    if nombre_columnas:
        print("Uniendo las partes de 'nombre' en una sola columna")
        
        # Crear una función para unir partes del nombre
        def construir_nombre(row):
            # Unir las partes con un espacio
            return ' '.join([str(part) for part in row[nombre_columnas] if pd.notna(part)])

        # Aplicar la función para construir el nombre
        df['name'] = df.apply(construir_nombre, axis=1)

        # Eliminar las partes del nombre
        df.drop(columns=nombre_columnas, inplace=True)
        
        print("Columna 'name' reconstruida:")
        print(df['name'].head())  # Imprimir los primeros valores de la nueva columna 'name' para verificar
    else:
        print("Advertencia: No se pudo unir 'name' porque faltan columnas necesarias.")

    return df

# Función para agregar la fila de encabezados y organizar la matriz final
def agregar_encabezados_y_organizar(matriz, permutacion, columnas_a_mezclar):
    print("Organizando matriz final con encabezados y columnas mezcladas")
    
    num_filas = len(matriz)
    num_columnas = len(matriz[0])
    
    matriz_permutada_indices = []  #vamos añadiendo aqui los valores permutados(sin encabezado-> lo añadimos al final)
    
    for fila in range(num_filas-1):
        nueva_fila = [None] * num_columnas
        for col in range(num_columnas):
            if col in columnas_a_mezclar:
                # Sustituir el índice por el valor original en la tabla
                indice_permutado = permutacion[fila][col]
                nueva_fila[col] = matriz[indice_permutado][col]
            else:
                nueva_fila[col] = matriz[fila + 1][col]
        matriz_permutada_indices.append(nueva_fila)
    print("Matriz organizada")
    return matriz_permutada_indices


# Función principal que llama al resto de funciones
def permutar_filas(matriz, columnas_a_mezclar, max_intentos=5):
    print(f"Permutando filas. Columnas a mezclar: {columnas_a_mezclar}")
    
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
    print(f"\nLa matriz de valores permutados se ha guardado en {archivo_salida}.")

# Función main que coordina la lectura, permutación y guardado
def main():
    print("Iniciando proceso de permutación")
    
    # Leer la matriz desde un archivo CSV
    ruta_de_trabajo = 'C:/Users/olivia.castineiras/Desktop/'
    nombre_fichero = 'cor_users.csv'  
    archivo_csv = ruta_de_trabajo + nombre_fichero
    df_original = pd.read_csv(archivo_csv)
    print(f"Archivo leído: {archivo_csv}")
    
    # Imprimir las columnas disponibles en el DataFrame
    print("Columnas en el DataFrame original:", df_original.columns.tolist())

    log.basicConfig(level=log.DEBUG)

    # Separar la columna 'email' si existe
    columnas_a_mezclar = ['id_user', 'email','name','id_contact']  # Especificar manualmente las columnas a mezclar

    # Si 'email' está en las columnas a mezclar, separamos primero
    if 'email' in columnas_a_mezclar:
        df_original, num_partes_nombre = separar_emails(df_original)  # Ahora también obtenemos el número de partes
        print(f"Columnas a mezclar después de separar 'email': {columnas_a_mezclar}")
        
        # Eliminar 'email' de las columnas a mezclar y añadir las nuevas columnas de nombre y dominio
        columnas_a_mezclar.remove('email')
        columnas_a_mezclar += [f'anterior_{i+1}' for i in range(num_partes_nombre)]+['domain']
        print("Columnas después de borrar 'email' y añadir las otras:", columnas_a_mezclar)

    # Separar la columna 'name' si existe
    df_original, num_partes_nombre_name = separar_nombre(df_original)
    if num_partes_nombre_name > 0:
        columnas_a_mezclar += [f'nombre_{i+1}' for i in range(num_partes_nombre_name)]  # Añadir las columnas a mezclar
        columnas_a_mezclar.remove('name')
        print(f"Columnas a mezclar después de separar 'name': {columnas_a_mezclar}")

    # Convertir DataFrame en matriz (lista de listas) para mezclar
    matriz_original = df_original.values.tolist()
    print("DataFrame convertido a matriz (lista de listas)")
    # print(matriz_original)

    # Obtener índices de las columnas a mezclar en la matriz
    indices_a_mezclar = []
    for col in columnas_a_mezclar:
        if col in df_original.columns:
            index = df_original.columns.get_loc(col)
            indices_a_mezclar.append(index)
        else:
            print(f"Advertencia: La columna '{col}' no se encuentra en el DataFrame.")
    
    print(f"Índices de las columnas a mezclar: {indices_a_mezclar}")

    try:
        # Permutar las filas de las columnas especificadas
        matriz_permutada_indices = permutar_filas(matriz_original, indices_a_mezclar)

        # Convertir la matriz permutada de nuevo a DataFrame
        df_permutado = pd.DataFrame(matriz_permutada_indices, columns=df_original.columns)
        print("Matriz permutada convertida a DataFrame")
        
        # Volver a unir las partes del email si existían
        if 'domain' in df_permutado.columns:
            df_permutado = unir_emails(df_permutado, num_partes_nombre)
            print("Emails unidos nuevamente")
        
        # Volver a unir las partes del nombre si existían
        df_permutado = unir_nombres(df_permutado)
        
        # Guardar la matriz permutada en un nuevo archivo CSV
        archivo_salida = 'C:/Users/olivia.castineiras/Desktop/permutado_' + nombre_fichero  
        df_permutado.to_csv(archivo_salida, index=False)
        print(f"Archivo guardado en: {archivo_salida}")

    except ValueError as e:
        print(e)

if __name__ == "__main__":
    main()
