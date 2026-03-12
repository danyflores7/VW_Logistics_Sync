import pandas as pd
import sqlite3
import os

def load_and_process_besi(file_path):
    """
    Paso 1: Carga y procesa el archivo de Demanda Besi.
    Extrae las columnas clave ('Noparte', 'TME', 'DAILY') y limpia los espacios en blanco
    de la columna 'Noparte' para facilitar futuros joins.
    """
    try:
        # Detectar el tipo de archivo según su extensión
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
            
        # Extraer específicamente las columnas requeridas (Noparte, TME, DAILY y fechas)
        import re
        
        # Encontrar todas las columnas que sean fechas (dd/mm/yyyy)
        date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
        date_columns = [col for col in df.columns if date_pattern.match(str(col))]
        
        # Columnas a conservar
        cols_to_keep = ['Noparte', 'TME']
        if 'DAILY' in df.columns:
            cols_to_keep.append('DAILY')
        cols_to_keep.extend(date_columns)
        
        df = df[cols_to_keep].copy()
        
        # Limpiar espacios en blanco al inicio y al final de la columna 'Noparte'
        df['Noparte'] = df['Noparte'].astype(str).str.strip()
        
        # Eliminar las filas basura donde Noparte es 'nan', etc.
        # Solo conservamos filas donde alguna de las columnas de fecha tenga datos
        if date_columns:
            df = df.dropna(subset=date_columns, how='all')
        elif 'DAILY' in df.columns:
            df = df.dropna(subset=['DAILY'])
        
        return df
    except KeyError as e:
        print(f"Error de columnas en Demanda Besi: {e}. Revisa que las columnas existan exactamente con esos nombres.")
        return None
    except Exception as e:
        print(f"Error general leyendo Demanda Besi ({file_path}): {e}")
        return None

def load_and_process_empaques(file_path):
    """
    Paso 2: Carga y procesa el archivo de Empaques AKSYS.
    Extrae las columnas necesarias y renombra 'NPsinEsp' a 'Noparte' para
    homologar el identificador con la tabla de Demanda.
    """
    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
            
        # Extraer las columnas clave
        df = df[['NPsinEsp', 'TIPO DE EMPAQUE', 'CAPACIDAD X EMPAQUE']]
        
        # Renombrar 'NPsinEsp' a 'Noparte' para alinearlo con Demanda Besi
        df = df.rename(columns={'NPsinEsp': 'Noparte'})
        
        return df
    except KeyError as e:
        print(f"Error de columnas en Empaques AKSYS: {e}. Revisa que los nombres coincidan.")
        return None
    except Exception as e:
        print(f"Error general leyendo Empaques AKSYS ({file_path}): {e}")
        return None

def load_and_process_plegados(file_path):
    """
    Paso 3: Carga y procesa el catálogo físico de Empaques Plegados.
    Extrae características del empaque, renombra el ID a 'TIPO DE EMPAQUE' y
    descarta (filtra) cualquier registro que no cuente con un ID.
    """
    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path, header=1)
        else:
            df = pd.read_excel(file_path, header=1)
            
        # Columnas clave a extraer
        cols = ['VACIOS_ ID', 'Colapsable / No colapsable', 'Largo m', 'Ancho m', 'Alto m', 'Altura plegada', 'Peso max kg']
        df = df[cols]
        
        # Renombrar el identificador para mapear con los otros archivos
        df = df.rename(columns={'VACIOS_ ID': 'TIPO DE EMPAQUE'})
        
        # Filtrar cualquier fila que tenga valores nulos en la columna 'TIPO DE EMPAQUE'
        df = df.dropna(subset=['TIPO DE EMPAQUE'])
        
        return df
    except KeyError as e:
        print(f"Error de columnas en Empaques Plegados: {e}. Revisa que los nombres coincidan.")
        return None
    except Exception as e:
        print(f"Error general leyendo Empaques Plegados ({file_path}): {e}")
        return None

def main():
    # Rutas absolutas a los archivos de origen usando el Workspace del usuario
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    file_besi = os.path.join(base_dir, 'Besi_Proveedor_Aksys .xlsx')
    file_empaques = os.path.join(base_dir, 'EMPAQUES AKSYSxls.xls')
    file_plegados = os.path.join(base_dir, 'empaques+plegadosxlsx (1).xlsx')
    
    db_name = os.path.join(base_dir, 'logistica_vw.db')
    
    print("Iniciando pipeline de datos para Volkswagen (MVP)...")
    
    # Procesamiento de DataFrames
    df_besi = load_and_process_besi(file_besi)
    df_empaques = load_and_process_empaques(file_empaques)
    df_plegados = load_and_process_plegados(file_plegados)
    
    total_registros = 0
    
    # Paso 4: Creación de la base de datos relacional y poblaciones de tablas
    try:
        # Se conecta (creando si no existe) a la base de datos local SQLite
        conn = sqlite3.connect(db_name)
        
        # Guardar DataFrame Besi
        if df_besi is not None:
            df_besi.to_sql('demanda_besi', conn, if_exists='replace', index=False)
            total_registros += len(df_besi)
            print(f"- Tabla 'demanda_besi' creada exitosamente ({len(df_besi)} filas).")
            
        # Guardar DataFrame Empaques AKSYS
        if df_empaques is not None:
            df_empaques.to_sql('empaques_aksys', conn, if_exists='replace', index=False)
            total_registros += len(df_empaques)
            print(f"- Tabla 'empaques_aksys' creada exitosamente ({len(df_empaques)} filas).")
            
        # Guardar DataFrame Empaques Plegados
        if df_plegados is not None:
            df_plegados.to_sql('empaques_plegados', conn, if_exists='replace', index=False)
            total_registros += len(df_plegados)
            print(f"- Tabla 'empaques_plegados' creada exitosamente ({len(df_plegados)} filas).")
            
        # Cerrar conexión
        conn.close()
        
        # Mensaje final solicitado
        print(f"\nBase de datos logistica_vw.db creada exitosamente con {total_registros} registros")
        
    except Exception as e:
        print(f"Error de base de datos SQLite: {e}")

def procesar_y_guardar_demanda(file_path: str, db_name: str = 'logistica_vw.db') -> int:
    """
    Función expuesta para FastAPI que procesa dinámicamente el archivo de demanda
    subido por el usuario y actualiza la tabla 'demanda_besi' en SQLite.
    Retorna el número de registros actualizados o -1 en caso de error.
    """
    try:
        print(f"Procesando nueva demanda desde archivo: {file_path}")
        df_besi = load_and_process_besi(file_path)
        
        if df_besi is not None:
            conn = sqlite3.connect(db_name)
            df_besi.to_sql('demanda_besi', conn, if_exists='replace', index=False)
            total_registros = len(df_besi)
            conn.close()
            print(f"- Tabla 'demanda_besi' actualizada exitosamente ({total_registros} filas).")
            return total_registros
        else:
            print("El dataframe de Demanda Besi resultó vacío.")
            return -1
            
    except Exception as e:
        print(f"Error actualizando demanda dinámica: {e}")
        return -1

if __name__ == "__main__":
    main()
