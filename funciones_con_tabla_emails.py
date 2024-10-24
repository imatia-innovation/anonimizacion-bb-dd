# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 11:48:24 2024

@author: olivia.castineiras
"""


import mysql.connector
import random
import pandas as pd
import logging as log

# Función para crear la matriz de permutación inicial
def crear_matriz_permutacion(num_filas, num_columnas):
    log.debug(f"Creando matriz de permutación inicial con {num_filas} filas y {num_columnas} columnas")
    permutacion= [[None] * num_columnas for _ in range(num_filas)] 
    return permutacion

# Función para mezclar los índices de una columna específica
def mezclar_indices_columna(permutacion, num_filas, col, max_intentos=5):
    indices_disponibles = list(range(0,num_filas))  # Omite la primera fila (encabezados)
    #print(indices_disponibles)
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


def separar_emails(df):
    if 'email' in df.columns:
        log.debug("Separando la columna 'email' en partes")
        
        # Dividir la columna "email" en nombre y dominio
        df[['anterior', 'domain']] = df['email'].str.split('@', n=1, expand=True)
        df['domain'] = '@' + df['domain'].astype(str)  # Añadir '@' al dominio
        
        # Separar 'name' por "." o "_"
        name_parts = df['anterior'].str.split(r'[._]', expand=True)

        # Identificar filas donde no se puede dividir (es decir, solo hay 'anterior_1')
        # Si todas las columnas, excepto 'anterior_1', son nulas, mezclamos los caracteres de 'anterior_1'
        for i, row in name_parts.iterrows():
            if row[1:].isnull().all():  # Si desde anterior_2 en adelante todo es nulo
                anterior_mezclado = ''.join(random.sample(row[0], len(row[0])))  # Mezclar los caracteres de anterior_1
                name_parts.at[i, 0] = anterior_mezclado

        # Renombrar las columnas de name_parts como anterior_1, anterior_2, ...
        name_parts.columns = [f'anterior_{i+1}' for i in range(name_parts.shape[1])]

        # Pone las partes en las que dividimos 'name' al final del df
        df = pd.concat([df.drop(columns=['email', 'anterior']), name_parts], axis=1)  # Eliminar 'name' y 'email'

        # Reorganizar las columnas para que 'domain' sea la última columna
        cols = df.columns.tolist()  # Obtener la lista de columnas
        cols.append(cols.pop(cols.index('domain')))  # Mover 'domain' al final
        df = df[cols]  # Reorganizar el DataFrame
        
        # Devolver el DataFrame y el número de partes del nombre
        num_partes_nombre = name_parts.shape[1]
        log.debug(f"Número de partes del nombre: {num_partes_nombre}")
        return df, num_partes_nombre

    return df, 0  # Si no existe 'email', devuelve el dataframe sin cambios y 0 partes



def unir_emails(df, num_partes_nombre):
    # Generar nombres de columnas para partes del nombre
    nombre_columnas = [f'anterior_{i+1}' for i in range(num_partes_nombre)]
    
    # Comprobar si la columna 'domain' y todas las partes del nombre existen
    if 'domain' in df.columns and all(col in df.columns for col in nombre_columnas):
        log.debug(f"Uniendo las partes de 'email'. Número de partes del nombre: {num_partes_nombre}")

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
        
        log.debug("Columna 'email' reconstruida:")
        log.debug(df['email'].head())  # Imprimir los primeros valores de la nueva columna 'email' para verificar
    else:
        missing_columns = [col for col in nombre_columnas if col not in df.columns] + (['domain'] if 'domain' not in df.columns else [])
        log.debug(f"Advertencia: No se pudo unir 'email' porque faltan columnas necesarias: {missing_columns}")

    return df

# Nueva función para separar el nombre en partes y mezclar
def separar_nombre(df):
    if 'name' in df.columns:
        log.debug("Separando la columna 'name' en partes")
        
        # Dividir la columna "name" en tantas partes como sea necesario
        name_parts = df['name'].str.split(r'[ .]', expand=True)  # Separar por espacio

        # Renombrar las columnas de name_parts como name_1, name_2, ...
        name_parts.columns = [f'nombre_{i+1}' for i in range(name_parts.shape[1])]

        # Pone las partes en las que dividimos 'name' al final del df 
        df = pd.concat([df.drop(columns=['name']), name_parts], axis=1)  # Eliminar 'name'

        # Reorganizar las columnas para que la última sea la más baja
        cols = df.columns.tolist()  # Obtener la lista de columnas
        df = df[cols]  # Reorganizar el DataFrame
        
        # Devolver el DataFrame y el número de partes del nombre
        num_partes_nombre = name_parts.shape[1]
        log.debug(f"Número de partes del nombre: {num_partes_nombre}")
        return df, num_partes_nombre

    return df, 0  # Si no existe 'name', devuelve el dataframe sin cambios y 0 partes

def unir_nombres(df):
    # Generar nombres de columnas para partes del nombre
    nombre_columnas = [col for col in df.columns if col.startswith('nombre_')]
    
    # Comprobar si existen partes del nombre
    if nombre_columnas:
        log.debug("Uniendo las partes de 'nombre' en una sola columna")
        
        # Crear una función para unir partes del nombre
        def construir_nombre(row):
            # Unir las partes con un espacio
            return ' '.join([str(part) for part in row[nombre_columnas] if pd.notna(part)])

        # Aplicar la función para construir el nombre
        df['name'] = df.apply(construir_nombre, axis=1)

        # Eliminar las partes del nombre
        df.drop(columns=nombre_columnas, inplace=True)
        
        log.debug("Columna 'name' reconstruida:")
        log.debug(df['name'].head())  # Imprimir los primeros valores de la nueva columna 'name' para verificar
    else:
        log.debug("Advertencia: No se pudo unir 'name' porque faltan columnas necesarias.")

    return df


# Nueva función para separar el nombre en partes y mezclar
def separar_surname(df):
    if 'surname' in df.columns:
        log.debug("Separando la columna 'surname' en partes")
        
        # Dividir la columna "name" en tantas partes como sea necesario
        surname_parts = df['surname'].str.split(r'[ .]', expand=True)  # Separar por espacio

        # Renombrar las columnas de name_parts como name_1, name_2, ...
        surname_parts.columns = [f'surname_{i+1}' for i in range(surname_parts.shape[1])]

        # Pone las partes en las que dividimos 'name' al final del df 
        df = pd.concat([df.drop(columns=['surname']), surname_parts], axis=1)  # Eliminar 'name'

        # Reorganizar las columnas para que la última sea la más baja
        cols = df.columns.tolist()  # Obtener la lista de columnas
        df = df[cols]  # Reorganizar el DataFrame
        
        # Devolver el DataFrame y el número de partes del nombre
        num_partes_surname = surname_parts.shape[1]
        log.debug(f"Número de partes del nombre: {num_partes_surname}")
        return df, num_partes_surname

    return df, 0  # Si no existe 'name', devuelve el dataframe sin cambios y 0 partes

def unir_surname(df):
    # Generar nombres de columnas para partes del nombre
    nombre_columnas = [col for col in df.columns if col.startswith('surname_')]
    
    # Comprobar si existen partes del nombre
    if nombre_columnas:
        log.debug("Uniendo las partes de 'surname' en una sola columna")
        
        # Crear una función para unir partes del nombre
        def construir_nombre(row):
            # Unir las partes con un espacio
            return ' '.join([str(part) for part in row[nombre_columnas] if pd.notna(part)])

        # Aplicar la función para construir el nombre
        df['surname'] = df.apply(construir_nombre, axis=1)

        # Eliminar las partes del nombre
        df.drop(columns=nombre_columnas, inplace=True)
        
        log.debug("Columna 'surname' reconstruida:")
        log.debug(df['surname'].head())  # Imprimir los primeros valores de la nueva columna 'name' para verificar
    else:
        log.debug("Advertencia: No se pudo unir 'surname' porque faltan columnas necesarias.")

    return df



# Función para agregar la fila de encabezados y organizar la matriz final
def agregar_encabezados_y_organizar(matriz, permutacion, columnas_a_mezclar):
    log.debug("Organizando matriz final con encabezados y columnas mezcladas")
    
    num_filas = len(matriz)
    num_columnas = len(matriz[0])
    
    matriz_permutada_indices = []  #vamos añadiendo aqui los valores permutados(sin encabezado-> lo añadimos al final)
    
    for fila in range(num_filas): 
        nueva_fila = [None] * num_columnas
        for col in range(num_columnas):
            if col in columnas_a_mezclar:
                # Sustituir el índice por el valor original en la tabla
                indice_permutado = permutacion[fila][col]
                nueva_fila[col] = matriz[indice_permutado][col]
            else:
                nueva_fila[col] = matriz[fila][col]
        matriz_permutada_indices.append(nueva_fila)
        
    return matriz_permutada_indices


# Función principal que llama al resto de funciones
def permutar_filas(matriz, columnas_a_mezclar, max_intentos=5):
    log.debug(f"Permutando filas. Columnas a mezclar: {columnas_a_mezclar}")
    
    num_filas = len(matriz)
    num_columnas = len(matriz[0])
    
    # Crea la matriz de permutación inicial
    permutacion = crear_matriz_permutacion(num_filas, num_columnas)
    
    # Itera sobre las columnas especificadas y mezcla los índices
    for col in columnas_a_mezclar:
        permutacion = mezclar_indices_columna(permutacion, num_filas, col, max_intentos)
    
    # Organiza la matriz final con encabezados y las columnas mezcladas
    matriz_permutada_indices = agregar_encabezados_y_organizar(matriz, permutacion, columnas_a_mezclar)
    log.debug("MATRIZ LISTA")
    return matriz_permutada_indices



# Conexión a la base de datos
def conectar_bd():
    log.debug("Conectando a BBDD: ")
    cnx = mysql.connector.connect(
        user='olivia',
        password='olivia',
        host='localhost',
        database='tests_anonimizacion'
    ) 
    return cnx

# Obtener las tablas de la base de datos
def obtener_tablas(cnx):
    cursor = cnx.cursor()
    cursor.execute("SHOW TABLES")
    tablas = cursor.fetchall()
    return [tabla[0] for tabla in tablas]

# Función para capturar errores en la entrada de un número
def solicitar_numero(mensaje, min_valor=None, max_valor=None):
    while True:
        try:
            valor = int(input(mensaje))
            if min_valor is not None and valor < min_valor:
                print(f"Error: El número debe ser mayor o igual a {min_valor}.")
            elif max_valor is not None and valor > max_valor:
                print(f"Error: El número debe ser menor o igual a {max_valor}.")
            else:
                return valor
        except ValueError:
            print("Error: Por favor, ingrese un número válido.")

# Función para capturar errores en la entrada de texto
def solicitar_opcion(mensaje, opciones_validas):
    while True:
        opcion = input(mensaje).strip().lower()
        if opcion in opciones_validas:
            return opcion
        else:
            print(f"Error: Opción no válida. Las opciones válidas son: {', '.join(opciones_validas)}")

# Función principal para interactuar con el usuario
def interfaz_usuario():
    cnx = conectar_bd()

    while True:
        print("\n=== Menú Principal ===")
        print("1. Mezclar una tabla")
        print("2. Salir")

        opcion = solicitar_opcion("Seleccione una opción (1 o 2): ", ['1', '2'])

        if opcion == "1":
            tablas = obtener_tablas(cnx)  # Obtener las tablas de la BD
            log.debug("\nTablas disponibles:")
            for idx, tabla in enumerate(tablas):
                print(f"{idx + 1}. {tabla}")

            seleccion_tabla = solicitar_numero("Seleccione la tabla que desea mezclar (número): ", 1, len(tablas)) - 1
            tabla_seleccionada = tablas[seleccion_tabla]
            df = leer_tabla(cnx, tabla_seleccionada)  # Leer la tabla seleccionada

            print("\nColumnas disponibles en la tabla:")
            columnas = df.columns.tolist()
            for idx, col in enumerate(columnas):
                print(f"{idx + 1}. {col}")

            while True:
                columnas_a_mezclar = input("\nIngrese los números de las columnas que desea mezclar (separados por comas): ")
                try:
                    columnas_a_mezclar = [columnas[int(idx) - 1] for idx in columnas_a_mezclar.split(",")]
                    break  # Si el input es correcto, salimos del bucle
                except (ValueError, IndexError):
                    print("Error: Ingrese números válidos correspondientes a las columnas.")

            columnas_tipo = {}  # Diccionario para almacenar los tipos de columnas
            for columna in columnas_a_mezclar:
                tipo = solicitar_opcion(f"\n¿Qué tipo de datos es la columna '{columna}'? (name, surname, email, otro): ", ['name', 'surname', 'email', 'otro'])
                columnas_tipo[columna] = tipo

            df_mezclado = aplicar_mezcla_en_partes(df, columnas_tipo)

            clave_primaria = input("Por favor, introduce el nombre de la columna que es la clave primaria, si no tiene o no la sabes introduce NO: ").strip()
            # Guardar en la BD la tabla mezclada
            guardar_en_bd(df_mezclado, cnx, tabla_seleccionada, columnas_a_mezclar, clave_primaria)

        elif opcion == "2":
            print("Saliendo del programa.")
            cnx.close()
            break

def solicitar_numero_o_texto(mensaje, opciones_validas=None):
    while True:
        entrada = input(mensaje).strip()
        if opciones_validas and entrada.lower() in opciones_validas:
            return entrada.lower()
        elif opciones_validas:
            log.debug(f"Opción no válida. Opciones válidas: {', '.join(opciones_validas)}")
        else:
            return entrada

# Función modificada para leer la tabla con paginación
def leer_tabla(cnx, tabla_seleccionada, batch_size=100):
    try:
        # Crear un DataFrame vacío para almacenar los resultados
        df_total = pd.DataFrame()
        offset = 0

        while True:
            query = f"SELECT * FROM {tabla_seleccionada} LIMIT {batch_size} OFFSET {offset}"
            df_batch = pd.read_sql(query, cnx)

            if df_batch.empty:
                break  # Salir del bucle si no hay más datos

            df_total = pd.concat([df_total, df_batch], ignore_index=True)
            offset += batch_size  # Aumentar el offset para la próxima iteración

        log.debug(f"Datos leídos desde la tabla {tabla_seleccionada}")
        return df_total
    except Exception as e:
        log.debug(f"Error al leer la tabla {tabla_seleccionada}: {str(e)}")
        return pd.DataFrame()  # Devolver DataFrame vacío en caso de error

def aplicar_mezcla_en_partes(df, columnas_tipo, batch_size=100):
    # Inicializa las listas para las columnas a mezclar
    columnas_a_mezclar = []

    # Separar las columnas por su tipo
    for columna, tipo in columnas_tipo.items():
        if tipo == 'email':
            df, num_partes_email = separar_emails(df)
            columnas_a_mezclar += [f'anterior_{i+1}' for i in range(num_partes_email)] + ['domain']
        elif tipo == 'name':
            df, num_partes_nombre = separar_nombre(df)
            columnas_a_mezclar += [f'nombre_{i+1}' for i in range(num_partes_nombre)]
        elif tipo == 'surname':
            df, num_partes_surname = separar_surname(df)
            columnas_a_mezclar += [f'surname_{i+1}' for i in range(num_partes_surname)]
        else:
            columnas_a_mezclar.append(columna)

    # Crear un DataFrame vacío para almacenar los resultados
    df_total = pd.DataFrame()

    total_filas = len(df)
    log.debug(f"Total filas df {total_filas}")
    for offset in range(0, total_filas, batch_size):
        df_batch = df.iloc[offset:offset + batch_size]
        
        # Convertir DataFrame en matriz (lista de listas) para mezclar
        matriz_original = df_batch.values.tolist()

        # Obtener índices de las columnas a mezclar
        indices_a_mezclar = [df.columns.get_loc(col) for col in columnas_a_mezclar]

        # Permutar las filas de las columnas especificadas
        matriz_permutada_indices = permutar_filas(matriz_original, indices_a_mezclar)

        # Convertir la matriz permutada de nuevo a DataFrame
        df_permutado = pd.DataFrame(matriz_permutada_indices, columns=df_batch.columns)

        # Volver a unir las partes de los nombres, apellidos y emails si existen
        if 'domain' in df_permutado.columns:
            df_permutado = unir_emails(df_permutado, num_partes_email)
        if 'nombre_1' in df_permutado.columns:
            df_permutado = unir_nombres(df_permutado)
        if 'surname_1' in df_permutado.columns:
            df_permutado = unir_surname(df_permutado)

        # Concatenar el DataFrame procesado a los resultados totales
        df_total = pd.concat([df_total, df_permutado], ignore_index=True)

    return df_total

def guardar_en_bd(df, cnx, tabla, columnas_a_mezclar, clave_primaria, batch_size=100):
    log.debug(f"Actualizando datos en la tabla {tabla}")
    cursor = cnx.cursor()
    
    total_filas = len(df)
    log.debug("actualizando paginas")
    for offset in range(0, total_filas, batch_size):
        # Obtener el lote de filas
        df_batch = df.iloc[offset:offset + batch_size]
        log.debug(f"Procesando filas desde {offset} hasta {min(offset + batch_size, total_filas)}")

            # Construir la consulta con clave primaria
        for index, row in df_batch.iterrows():
            log.debug(f"Procesando fila {index} del batch {offset}")
            set_clauses = ", ".join([f"{col} = %s" for col in columnas_a_mezclar])
            update_query = f"UPDATE {tabla} SET {set_clauses} WHERE {clave_primaria} = %s"

            # Extraer los valores para las columnas y la clave primaria
            valores = [None if pd.isna(row[col]) else row[col] for col in columnas_a_mezclar]
            clave_valor = row[clave_primaria]

            # Ejecutar la consulta para cada fila individualmente
            try:
                cursor.execute(update_query, valores + [clave_valor])
                cnx.commit()
            except mysql.connector.Error as e:
                log.error(f"Error actualizando la fila {index + offset}: {e.msg}")



    print("Datos actualizados en la base de datos.")





if __name__ == "__main__":
    log.basicConfig(
        level=log.DEBUG,
        format='%(asctime)s %(levelname)-8s %(message)s',
    )
    interfaz_usuario()
