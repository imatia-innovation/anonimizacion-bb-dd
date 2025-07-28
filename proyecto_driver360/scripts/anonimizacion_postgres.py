import psycopg2
from psycopg2 import sql
import csv
import os
import random
import time
from datetime import datetime
import sys

### MACROS ###

TAM_PAGINA = 500000
ARCHIVO_ESTADO = "estado.txt"
RUTA_TABLAS_ANONIMIZABLES = "scripts/tablas.csv"

### ESTADO DE LA EJECUCIÓN Y CONFIGURACIÓN ###

def conexion_postgres():
    try:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST", "localhost"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", "1234"),
            dbname=os.environ.get("DB_NAME", "demo_driver360_copia"),
            port=int(os.environ.get("DB_PORT", 5432))
        )
        cursor = conn.cursor()
        return conn, cursor
    except psycopg2.Error as error:
        print(f"Error de conexión: {error}", flush=True)
        return None, None


def leer_estado():
    try:
        with open(ARCHIVO_ESTADO, "r") as f:
            estado = f.read().strip().split(",")
            tabla_indice, offset = estado[0], int(estado[1])
            return tabla_indice, offset
    except Exception as e:
        print(e, flush=True)
        return None, 0


def guardar_estado(tabla_origen, offset):
    with open(ARCHIVO_ESTADO, "w") as f:
        f.write(f"{tabla_origen},{offset}")


def borrar_estado():
    if os.path.exists(ARCHIVO_ESTADO):
        os.remove(ARCHIVO_ESTADO)


### MÉTODOS AUXILIARES ###
def obtener_columnas(cursor, tabla_origen):
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
    """, (tabla_origen, ))
    columnas = [fila[0] for fila in cursor.fetchall()]
    if not columnas:
        print(f"Warning: No se encontraron columnas para tabla '{tabla_origen}' en esquema 'public'")
    return columnas


def obtener_tablas_anonimizables():
    tablas_anonimizables = []
    with open(RUTA_TABLAS_ANONIMIZABLES, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for i, fila in enumerate(reader):
            tabla = fila[0].strip()
            tablas_anonimizables.append((tabla, i))
    return tablas_anonimizables


### MÉTODOS DE ANONIMIZACIÓN ###
def eliminar_columnas(cursor, conn, tabla_origen, *columnas):
    #Elimino las columnas seleccionadas de la tabla
    for columna in columnas:
        try:
            cursor.execute(f"ALTER TABLE {tabla_origen} DROP COLUMN {columna};")
        except Exception as e:
            print(f"Error en tabla {tabla_origen} al eliminar la columna {columna}. Revisar si existía previamente. {e}", flush=True)
    conn.commit()


def reordenar_grupos_ip(cursor, conn, tabla_origen, *columnas):
    es_aud = tabla_origen.endswith('_aud')
    try:
        columna_id = obtener_columnas(cursor, tabla_origen)[0]
        for columna in columnas:
            if es_aud:
                columnas_ips = f"{columna_id}, rev_ver, {columna}"
            else:
                columnas_ips = f"{columna_id}, {columna}"

            cursor.execute(f"SELECT {columnas_ips} FROM {tabla_origen};")
            filas = cursor.fetchall()

            for fila in filas:
                if es_aud:
                    id_fila, rev_ver, ip = fila
                else:
                    id_fila, ip = fila
                    rev_ver = None

                if ip is None:
                    continue

                try:
                    grupos = str(ip).split(".")
                    if grupos is not None and len(grupos) != 4:
                        raise ValueError("Formato de IP inválido")
                    random.shuffle(grupos)
                    nueva_ip = ".".join(grupos)
                except Exception as e:
                    print(f"Error en IP {ip} (ID: {id_fila}, columna: {columna}): {e}", flush=True)
                    continue

                # Construir y ejecutar UPDATE
                if es_aud:
                    cursor.execute(
                        f"""UPDATE {tabla_origen} SET {columna} = %s WHERE {columna_id} = %s AND rev_ver = %s;""",
                        (nueva_ip, id_fila, rev_ver)
                    )
                else:
                    cursor.execute(
                        f"""UPDATE {tabla_origen} SET {columna} = %s WHERE {columna_id} = %s;""",
                        (nueva_ip, id_fila)
                    )

            conn.commit()

    except Exception as e:
        print(f"Error al reordenar las IPs en la tabla {tabla_origen}: {e}", flush=True)


def reordenar_antes_arroba(cursor, conn, tabla_origen, *columnas):
    #Nombre de la primera columna
    cursor.execute("""
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = %s
		ORDER BY ordinal_position;
	""", (tabla_origen,))
    es_aud = tabla_origen.endswith('_aud')
    columna_id = cursor.fetchall()[0][0]

    for columna in columnas:
        try:
            #Seleccionamos id y valor de columna
            if es_aud:
                cursor.execute(f"SELECT {columna_id}, rev_ver, {columna} FROM {tabla_origen};")
                filas = cursor.fetchall()
            else:
                cursor.execute(f"SELECT {columna_id}, {columna} FROM {tabla_origen};")
                filas = cursor.fetchall()

            for fila in filas:
                if es_aud:
                    id_fila, rev_ver, valor = fila
                else:
                    id_fila, valor = fila
                    rev_ver = None  # no se usa

                if valor is None:
                    continue

                #Busco posición de la arroba
                pos_arroba = valor.find('@')
                if pos_arroba == -1:
                    parte_izq = valor
                    parte_der = ''
                else:
                    parte_izq = valor[:pos_arroba]
                    parte_der = valor[pos_arroba:]

                #Reordeno aleatoriamente los caracteres de la parte izquierda de la arroba
                lista_chars = list(parte_izq) #Convierto el string en lista de chars
                random.shuffle(lista_chars)
                parte_izq_reordenada = ''.join(lista_chars) #Convierto la lista de chars back to string
                nuevo_valor = parte_izq_reordenada + parte_der

                # Actualizamos fila en la tabla
                if es_aud:
                    cursor.execute(f"""UPDATE {tabla_origen} SET {columna} = %s WHERE {columna_id} = %s AND rev_ver = %s;""",(nuevo_valor, id_fila, rev_ver))
                else:
                    cursor.execute(f"""UPDATE {tabla_origen} SET {columna} = %s WHERE {columna_id} = %s;""", (nuevo_valor, id_fila))
            conn.commit()

        except Exception as e:
            print(f"Error en tabla {tabla_origen} al reordenar el contenido de la columna {columna}, {e}", flush=True)
            break


def reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset_inicial, *columnas):
    total_columnas = obtener_columnas(cursor, tabla_origen)
    columna_id = total_columnas[0]
    es_aud = tabla_origen.endswith('_aud')
    exito = True

    offset_actual = int(offset_inicial) if offset_inicial else 0

    while True:
        try:
            if es_aud:
                cursor.execute(f"""
                    SELECT * FROM {tabla_origen}
                    ORDER BY {columna_id} ASC, rev_ver ASC
                    LIMIT %s OFFSET %s;
                """, (TAM_PAGINA, offset_actual))
            else:
                cursor.execute(f"""
                    SELECT * FROM {tabla_origen}
                    ORDER BY {columna_id} ASC
                    LIMIT %s OFFSET %s;
                """, (TAM_PAGINA, offset_actual))

            filas = cursor.fetchall()
            if not filas:
                break

            nuevas_filas = [list(fila) for fila in filas]

            for nombre_columna in columnas: #Todas las columnas de la tabla original
                idx_col = total_columnas.index(nombre_columna)
                valores_a_randomizar = [fila[idx_col] for fila in nuevas_filas] #Guardamos cada columna a randomizar en esta variable
                random.shuffle(valores_a_randomizar) #Randomizamos
                for i, fila in enumerate(nuevas_filas):
                    fila[idx_col] = valores_a_randomizar[i] #Loopeando sobre las filas, modificamos el valor de la columna que ha sido randomizada (la columna idx_col). nuevas_filas queda actualizado

            for fila in nuevas_filas: #Con nuevas_filas actualizado con los valores ya shuffleados para updatear, construimos la sentencia del update y la rulamos
                set_clause = ", ".join([f"{col} = %s" for col in columnas])
                valores = [fila[total_columnas.index(col)] for col in columnas] #Los valores ya anonimizados solamente de las columnas anonomizadas, para mandar al update

                if es_aud:
                    id_val = fila[total_columnas.index(columna_id)]
                    rev_val = fila[total_columnas.index("rev_ver")]
                    cursor.execute(
                        f"""
                        UPDATE {tabla_origen}
                        SET {set_clause}
                        WHERE {columna_id} = %s AND rev_ver = %s;
                        """,
                        valores + [id_val, rev_val]
                    ) #Valores aplica a los %s del set. Se concatena con los IDs para hacer 1 única lista para todos los %s
                else:
                    id_val = fila[total_columnas.index(columna_id)]
                    cursor.execute(
                        f"""
                        UPDATE {tabla_origen}
                        SET {set_clause}
                        WHERE {columna_id} = %s;
                        """,
                        valores + [id_val]
                    )

            conn.commit()

            offset_actual += TAM_PAGINA
            guardar_estado(tabla_origen, offset_actual)

        except Exception as e:
            print(f"Error en tabla {tabla_origen} desde offset {offset_actual}: {e}", flush=True)
            guardar_estado(tabla_origen, offset_actual)
            exito = False
            break

    if exito:
        borrar_estado()

    return offset_actual


def reordenar_bloques_columna_en_bloques(cursor, conn, tabla_origen, offset_inicial, *columnas):
    total_columnas = obtener_columnas(cursor, tabla_origen)
    columna_id = total_columnas[0]
    offset_actual = int(offset_inicial) if offset_inicial else 0
    exito = True
    es_aud = tabla_origen.endswith("_aud")
    columna_rev = "rev_ver"

    while True:
        try:
            if es_aud:
                cursor.execute(f"""
                    SELECT * FROM {tabla_origen}
                    ORDER BY {columna_id} ASC, {columna_rev} ASC
                    LIMIT %s OFFSET %s;
                """, (TAM_PAGINA, offset_actual))
            else:
                cursor.execute(f"""
                    SELECT * FROM {tabla_origen}
                    ORDER BY {columna_id} ASC
                    LIMIT %s OFFSET %s;
                """, (TAM_PAGINA, offset_actual))

            filas = cursor.fetchall()
            if not filas:
                break

            nuevas_filas = [list(fila) for fila in filas] #lista de listas

            indices_columnas = [total_columnas.index(col) for col in columnas]

            #Extraer bloques de valores a reordenar conjuntamente en forma de lista de tuplas
            bloques = [
                tuple(fila[i] for i in indices_columnas)
                for fila in nuevas_filas
            ]

            random.shuffle(bloques)

            #Construimos la fila nueva, manteniendo el orden original de las columnas
            for i, fila in enumerate(nuevas_filas):
                for j, idx in enumerate(indices_columnas):
                    fila[idx] = bloques[i][j] #modificamos solamente las columnas a anonimizar (idx son sus indices)

            for fila in nuevas_filas:
                set_clause = ", ".join([f"{col} = %s" for col in columnas])
                valores = [fila[total_columnas.index(col)] for col in columnas]

                if es_aud:
                    id_val = fila[total_columnas.index(columna_id)]
                    rev_val = fila[total_columnas.index("rev_ver")]
                    cursor.execute(
                        f"""
                        UPDATE {tabla_origen}
                        SET {set_clause}
                        WHERE {columna_id} = %s AND rev_ver = %s;
                        """,
                        valores + [id_val, rev_val]
                    )  # Valores aplica a los %s del set. Se concatena con los IDs para hacer 1 única lista para todos los %s
                else:
                    id_val = fila[total_columnas.index(columna_id)]
                    cursor.execute(
                        f"""
                        UPDATE {tabla_origen}
                        SET {set_clause}
                        WHERE {columna_id} = %s;
                        """,
                        valores + [id_val]
                    )

            conn.commit()
            offset_actual += TAM_PAGINA
            guardar_estado(tabla_origen, offset_actual)

        except Exception as e:
            print(f"Error en tabla {tabla_origen} en offset {offset_actual}: {e}", flush=True)
            guardar_estado(tabla_origen, offset_actual)
            exito = False
            break

    if exito:
        borrar_estado()

    return offset_actual


### MAIN ###
def main():
    sys.stdout = open('logs.txt', 'w', encoding='utf-8')
    conn, cursor = conexion_postgres()

    #Lista de tablas a anonimizar
    tablas_anonimizables = obtener_tablas_anonimizables()
    #Estado de la ejecución anterior en caso de fallo
    estado_tabla, offset = leer_estado()

    for tabla_origen, orden in tablas_anonimizables:
        #Si la ejecución anterior dio error, saltamos todas las tablas anteriores a la última procesada
        if estado_tabla:
            #Buscamos el id de la tabla en el CSV para comparar
            orden_guardado = next((indice for tabla, indice in tablas_anonimizables if tabla == estado_tabla), None)
            if orden < orden_guardado:
                print(f"Saltando tabla {tabla_origen} con orden {orden} porque ya fue procesada", flush=True)
                continue #Saltamos las tablas ya procesadas
            elif orden == orden_guardado:
                print(f"Reanudando tabla {tabla_origen} desde ID {offset}", flush=True) #Se reanuda la tabla que produjo el error en la ejecución anterior, desde el id procesado
            else:
                offset = 0  #Para tablas posteriores empezamos desde 0
        else:
            offset = 0  #Sin estado, desde 0

        """
        if tabla_origen == "pruebas_reordenar_bloques_columna_en_bloques":
            reordenar_bloques_columna_en_bloques(cursor, conn, tabla_origen, offset,
                                                 *['direccion', 'piso', 'ciudad'])
        """

        if tabla_origen == "cor_assignment_contracts":
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            eliminar_columnas(cursor, conn, tabla_origen, *['client_document', 'insurance_policy_number', 'additional_document'])
            reordenar_antes_arroba(cursor, conn, tabla_origen, 'client_email', 'additional_email')
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['client_name', 'client_first_surname', 'client_second_surname',
                'client_birthday', 'client_phone', 'vehicle_plate', 'vehicle_vin', 'additional_phone'])
            reordenar_bloques_columna_en_bloques(cursor, conn, tabla_origen, offset, * ['company_address', 'company_center_address', 'client_address_street_name',
                'client_address_number', 'client_address_block', 'client_address_apartment', 'client_address_stair', 'client_address_province', 'client_address_council', 'client_address_postal_code',
                'additional_address_street_name', 'additional_address_number', 'additional_address_block', 'additional_address_apartment', 'additional_address_stair', 'additional_address_province',
                'additional_address_council','additional_address_postal_code'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen in ('cor_users', 'cor_users_aud'):
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            reordenar_antes_arroba(cursor, conn, tabla_origen, *['email', 'password', 'access_key'])
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, 'name')
            reordenar_grupos_ip(cursor, conn, tabla_origen, 'ip')
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(
                f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen in ('cor_thirds', 'cor_thirds_aud'):
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            eliminar_columnas(cursor, conn, tabla_origen, *['id_document', 'insurance_policy_number'])
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['name', 'surname'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(
                f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen in ('cor_contacts', 'cor_contacts_aud'):
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            eliminar_columnas(cursor, conn, tabla_origen, 'id_document')
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['name', 'surname', 'second_surname'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(
                f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen in ('cor_employees', 'cor_employees_aud'):
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            eliminar_columnas(cursor, conn, tabla_origen, 'id_document', 'social_security_number')
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['name', 'surname', 'birth_date'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen in ('cor_addresses', 'cor_addresses_aud'):
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            reordenar_bloques_columna_en_bloques(cursor, conn, tabla_origen, offset, *['latitude', 'longitude', 'street_name', 'number', 'apartment', 'stair', 'block', 'postal_code',
                'council', 'province'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen == 'cor_bank_accounts':
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['iban', 'bic'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen in ('cor_companies', 'cor_companies_aud'):
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            eliminar_columnas(cursor, conn, tabla_origen, 'id_document')
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['name', 'trade_register'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen == 'cor_credit_cards':
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['headline'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen == 'cor_free_invoices':
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            eliminar_columnas(cursor, conn, tabla_origen, 'buyer_id_document')
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['buyer_name', 'buyer_surname', 'buyer_second_surname',
                'buyer_phone_number1', 'buyer_phone_number2', 'buyer_phone_number3'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen in ('cor_notifications', 'cor_notifications_aud'):
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['message'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen in ('cor_phones', 'cor_phones_aud'):
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['number'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen == 'cor_tpv_data':
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            eliminar_columnas(cursor, conn, tabla_origen, *['fuc'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen == 'cor_invoice_delivery_notes':
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            eliminar_columnas(cursor, conn, tabla_origen, 'buyer_id_document', 'seller_id_document', 'buyer_id_document_country', 'seller_id_document_country')
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['buyer_name', 'buyer_surname', 'buyer_second_surname',
                 'buyer_phone_number1', 'buyer_phone_number2', 'buyer_phone_number3', 'seller_name', 'seller_surname', 'seller_second_surname'])
            reordenar_bloques_columna_en_bloques(cursor, conn, tabla_origen, offset, *['buyer_tax_addr_street_name', 'buyer_tax_addr_number', 'buyer_tax_addr_apartment',
                'buyer_tax_addr_stair', 'buyer_tax_addr_block', 'buyer_tax_addr_postal_code', 'buyer_tax_addr_council', 'buyer_tax_addr_province', 'buyer_tax_addr_id_country',
                'buyer_doc_addr_street_name', 'buyer_doc_addr_number', 'buyer_doc_addr_apartment', 'buyer_doc_addr_stair', 'buyer_doc_addr_block', 'buyer_doc_addr_postal_code', 'buyer_doc_addr_council',
                'buyer_doc_addr_province', 'buyer_doc_addr_id_country', 'seller_tax_addr_street_name', 'seller_tax_addr_number', 'seller_tax_addr_apartment', 'seller_tax_addr_stair',
                'seller_tax_addr_block', 'seller_tax_addr_postal_code', 'seller_tax_addr_council', 'seller_tax_addr_province', 'seller_tax_addr_id_country'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen == 'cor_invoice_reparation_orders':
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            eliminar_columnas(cursor, conn, tabla_origen, 'buyer_id_document', 'seller_id_document', 'buyer_id_document_country', 'seller_id_document_country')
            reordenar_antes_arroba(cursor, conn, tabla_origen, 'buyer_email', 'seller_email')
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['buyer_name', 'buyer_surname', 'buyer_second_surname',
                'buyer_phone_number1', 'buyer_phone_number2', 'buyer_phone_number3', 'seller_name', 'seller_surname', 'seller_second_surname'])
            reordenar_bloques_columna_en_bloques(cursor, conn, tabla_origen, offset, *['buyer_tax_addr_street_name', 'buyer_tax_addr_number', 'buyer_tax_addr_apartment',
                'buyer_tax_addr_stair', 'buyer_tax_addr_block', 'buyer_tax_addr_postal_code', 'buyer_tax_addr_council', 'buyer_tax_addr_province', 'buyer_tax_addr_id_country',
                'buyer_doc_addr_street_name', 'buyer_doc_addr_number', 'buyer_doc_addr_apartment', 'buyer_doc_addr_stair', 'buyer_doc_addr_block', 'buyer_doc_addr_postal_code',
                'buyer_doc_addr_council', 'buyer_doc_addr_province', 'buyer_doc_addr_id_country', 'seller_tax_addr_street_name', 'seller_tax_addr_number', 'seller_tax_addr_apartment',
                'seller_tax_addr_stair', 'seller_tax_addr_block', 'seller_tax_addr_postal_code', 'seller_tax_addr_council', 'seller_tax_addr_province', 'seller_tax_addr_id_country'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

        elif tabla_origen in ('cor_vehicle_plates', 'cor_vehicle_plates_aud'):
            inicio = time.perf_counter()
            timestamp_inicio = datetime.now()
            reordenar_columna_en_bloques(cursor, conn, tabla_origen, offset, *['vehicle_plate'])
            fin = time.perf_counter()
            timestamp_fin = datetime.now()
            print(f"Tabla {tabla_origen} procesada. Tiempo transcurrido: {fin - inicio:.2f} segundos. Inicio: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}. Fin: {timestamp_fin.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    conn.close()

if __name__ == "__main__":
    main()

