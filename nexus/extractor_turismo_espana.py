import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import re


def extraer_datos_turismo_espana():
    # Configuración de la base de datos
    DB_CONFIG = {
        'host': 'localhost',
        'database': 'nexus',
        'user': 'maxim',  # Cambiar
        'password': 'diganDar',  # Cambiar
        'port': 5432
    }

    # Mapeo de archivos a origen
    ARCHIVO_ORIGEN = {
        '02_espanoles_': 'total_espana',
        '03_andaluces_': 'andalucia',
        '04_resto_espana_': 'resto_espana'
    }

    # Mapeo de indicadores
    INDICADORES = {
        'Número de viajeros en establecimientos hoteleros': 'viajeros_hoteles',
        'Número de pernoctaciones en establecimientos hoteleros': 'pernoctaciones_hoteles',
        'Llegadas de pasajeros a aeropuertos andaluces': 'llegadas_aeropuertos',
        'Número de turistas (millones)': 'turistas_millones',
        'Estancia Media (número de días)': 'estancia_media_dias',
        'Gasto medio diario (euros)': 'gasto_medio_diario'
    }

    try:
        # Buscar archivos de España
        dir_in = './nexus/data/ultimos_datos_turisticos'
        archivos = []

        for prefijo in ARCHIVO_ORIGEN.keys():
            files = [f for f in os.listdir(dir_in)
                     if f.startswith(prefijo) and '.xls' in f and '~' not in f]
            archivos.extend([(os.path.join(dir_in, f), prefijo) for f in files])

        print(f"Encontrados {len(archivos)} archivos para procesar")

        # Conectar a PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        datos_procesados = []

        for archivo_path, prefijo in archivos:
            print(f"Procesando: {archivo_path}")

            # Determinar origen y fecha
            origen = ARCHIVO_ORIGEN[prefijo]
            nombre = os.path.basename(archivo_path).lower()

            # Extraer fecha del nombre
            año, mes = 2025, 5  # Default
            match = re.search(r"(may|jun|jul|ago|sep|oct|nov|dic)(\d{2})", nombre)
            if match:
                mes_map = {'may': 5, 'jun': 6, 'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12}
                mes = mes_map.get(match.group(1), 5)
                año = 2000 + int(match.group(2))

            # Leer Excel
            df = pd.read_excel(archivo_path, header=None)

            # Inicializar registro
            registro = {
                'año': año,
                'mes': mes,
                'origen': origen,
                'viajeros_hoteles': None,
                'pernoctaciones_hoteles': None,
                'llegadas_aeropuertos': None,
                'turistas_millones': None,
                'estancia_media_dias': None,
                'gasto_medio_diario': None
            }

            # Buscar indicadores en el dataframe
            for idx, row in df.iterrows():
                if len(row) >= 3 and pd.notna(row[1]):
                    texto_indicador = str(row[1]).strip()

                    for indicador_excel, campo_tabla in INDICADORES.items():
                        if indicador_excel in texto_indicador:
                            valor = row[2] if pd.notna(row[2]) and str(row[2]) != '-' else None
                            if valor is not None:
                                try:
                                    if isinstance(valor, str):
                                        valor = float(valor.replace(',', ''))
                                    registro[campo_tabla] = float(valor)
                                    print(f"  {campo_tabla}: {valor}")
                                except:
                                    continue
                            break

            datos_procesados.append(registro)

        # Insertar datos en la base de datos
        print("Insertando datos en la base de datos...")

        for registro in datos_procesados:
            # Eliminar registros existentes
            cursor.execute(
                "DELETE FROM turismo_espana WHERE año = %s AND mes = %s AND origen = %s",
                (registro['año'], registro['mes'], registro['origen'])
            )

            # Insertar nuevo registro
            cursor.execute("""
                INSERT INTO turismo_espana 
                (año, mes, origen, viajeros_hoteles, pernoctaciones_hoteles, 
                 llegadas_aeropuertos, turistas_millones, estancia_media_dias, gasto_medio_diario)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                registro['año'], registro['mes'], registro['origen'],
                registro['viajeros_hoteles'], registro['pernoctaciones_hoteles'],
                registro['llegadas_aeropuertos'], registro['turistas_millones'],
                registro['estancia_media_dias'], registro['gasto_medio_diario']
            ))

        conn.commit()
        print(f"Procesados {len(datos_procesados)} registros exitosamente")

        # Mostrar resultados
        cursor.execute("SELECT * FROM turismo_espana ORDER BY año, mes, origen")
        print("\nDatos en la tabla:")
        for row in cursor.fetchall():
            print(row)

    except Exception as e:
        print(f"Error: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    extraer_datos_turismo_espana()