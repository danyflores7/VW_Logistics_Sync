import sqlite3
import pandas as pd
import math
import argparse
import os

# --- CONSTRAINTS ---
# Dimensiones Camión DHL Estándar (metros)
TRUCK_LENGTH = 6.5
TRUCK_WIDTH = 2.5
TRUCK_HEIGHT = 2.4
TRUCK_MAX_WEIGHT_KG = 5000

def get_daily_demand(db_path, fecha=None):
    """
    Paso 1: Consulta SQL (El JOIN Maestro)
    Se conecta a SQLite y ejecuta un JOIN entre las 3 tablas.
    Devuelve un DataFrame con la demanda consolidada y los detalles físicos del empaque.
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Ojo que en `empaques_aksys` el campo de cruce con `empaques_plegados` es `TIPO DE EMPAQUE`
        # Ambos deben coincidir en string.
        # Comprobar qué columnas existen
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(demanda_besi)")
        columnas = [x[1] for x in cursor.fetchall()]
        
        import re
        from datetime import datetime as dt
        date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
        date_columns = [col for col in columnas if date_pattern.match(str(col))]
        
        # Resolver fecha a buscar
        hoy_str = dt.now().strftime('%d/%m/%Y')
        if not fecha or fecha == "DAILY":
            if hoy_str in date_columns:
                fecha = hoy_str
            elif date_columns:
                fecha = date_columns[0]
                
        # Si pasaron una fecha que no existe, omitir
        if fecha not in date_columns:
            print(f"La fecha requerida {fecha} no existe en la base de datos.")
            conn.close()
            return None
            
        col_fecha = f'"{fecha}"'
            
        query = f"""
        SELECT 
            b.Noparte, 
            b.TME, 
            b.{col_fecha} AS DAILY, 
            a."TIPO DE EMPAQUE", 
            a."CAPACIDAD X EMPAQUE", 
            p."Largo m", 
            p."Ancho m", 
            p."Alto m",
            p."Altura plegada", 
            p."Peso max kg"
        FROM demanda_besi b
        INNER JOIN empaques_aksys a ON b.Noparte = a.Noparte
        INNER JOIN empaques_plegados p ON a."TIPO DE EMPAQUE" = p."TIPO DE EMPAQUE"
        WHERE b.{col_fecha} > 0
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Limpiar posibles datos nulos o errores de formato importados
        # Convertir a flotantes para calculos matematicos limpios.
        for col in ['DAILY', 'CAPACIDAD X EMPAQUE', 'Largo m', 'Ancho m', 'Alto m', 'Altura plegada', 'Peso max kg']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Eliminar registros con errores en características clave
        df = df.dropna(subset=['DAILY', 'CAPACIDAD X EMPAQUE', 'Largo m', 'Ancho m', 'Alto m', 'Altura plegada'])
        
        return df
    except Exception as e:
        print(f"Error al obtener demanda de la base de datos: {e}")
        return None

def calculate_required_boxes(df):
    """
    Paso 2: Cálculo de Cajas Necesarias
    Fórmula: Cajas_Requeridas = CEIL(DAILY / CAPACIDAD X EMPAQUE)
    Se envían cajas enteras.
    """
    if df is None or df.empty:
        return df
        
    df['Cajas_Requeridas'] = df.apply(
        lambda row: math.ceil(row['DAILY'] / row['CAPACIDAD X EMPAQUE']) if row['CAPACIDAD X EMPAQUE'] > 0 else 0,
        axis=1
    )
    return df

def calculate_truck_occupancy(df):
    """
    Paso 3: Algoritmo de Cubicaje 3D Heurístico (El Núcleo)
    Calcula cuántas cajas colapsadas colapsables CABEN por TIPO en un camión estándar sin rotar (No Rotación Automotriz).
    Se agrupa por la base y el alto del camión.
    Devuelve el dataframe con dos nuevas columnas: 'Capacidad_Cajas_Camion' (Real) 
    y 'Cajas_Base' (Para propósitos analíticos).
    """
    if df is None or df.empty:
        return df

    def get_max_boxes_for_truck(row):
        try:
            l = float(row['Largo m'])
            w = float(row['Ancho m'])
            h_full = float(row['Alto m']) # Usar Alto normal (caja llena)
            w_kg = float(row['Peso max kg'])
            
            # Prevenir divisiones entre 0
            if l <= 0 or w <= 0 or h_full <= 0:
                return 0, 0
                
            # 1. ¿Cuántas caben en la "base" del camión orientadas de una única manera?
            # Se asume orientación a lo largo (Largo camión / Largo caja) x (Ancho camión / Ancho caja)
            cajas_base_lengthways = math.floor(TRUCK_LENGTH / l) * math.floor(TRUCK_WIDTH / w)
            
            # 2. ¿Cuántas caben de alto (apiladas)?
            cajas_alto = math.floor(TRUCK_HEIGHT / h_full)
            
            # 3. ¿Capacidad volumétrica teórica del camión (Max_Cajas_Por_Camion)?
            max_cajas_volumen = cajas_base_lengthways * cajas_alto
            
            # 4. Validar el peso del empaque lleno.
            if w_kg > 0:
                max_cajas_peso = math.floor(TRUCK_MAX_WEIGHT_KG / w_kg)
                # 5. La capacidad REAL será la restricción más fuerte (mínima)
                capacidad_real = min(max_cajas_volumen, max_cajas_peso)
            else:
                capacidad_real = max_cajas_volumen
                
            return int(capacidad_real), int(cajas_base_lengthways)
        except Exception:
            return 0, 0

    # Aplicar la función a todo el DF y separar resultados en dos columnas.
    df[['Capacidad_Cajas_Camion', 'Cajas_Base']] = df.apply(
        lambda row: pd.Series(get_max_boxes_for_truck(row)), 
        axis=1
    )
    
    return df

def generate_logistics_plan(df):
    """
    Paso 4: Consolidación y Salida
    Calcula camiones reales necesarios y agrupa un resumen del envío total.
    """
    if df is None or df.empty:
        return {"error": "No data to process"}
        
    resumen = {
        "Total_Cajas_A_Enviar": int(df['Cajas_Requeridas'].sum()),
        "Detalle_Por_Num_Parte": [],
        "Total_Camiones_Estimados": 0,
        "Advertencias": []
    }
    
    total_trucks = 0
    total_boxes_global = 0
    
    # Procesar línea por línea (por número de parte) 
    # asumiendo segregación de carga (1 número de parte puede necesitar 1 o más camiones compartimentados o exclusivos)
    # Para efectos MVP sumaremos los requerimientos de camiones por PN.
    for _, row in df.iterrows():
        pn = row['Noparte']
        cajas_req = row['Cajas_Requeridas']
        capacidad_truck = row['Capacidad_Cajas_Camion']
        
        if cajas_req == 0:
            continue
            
        if capacidad_truck == 0:
            resumen["Advertencias"].append(f"El empaque para el PN {pn} es más grande que el camión o pesa demasiado. Capacidad calculada: 0")
            continue
            
        # Cuántos camiones enteros (y parciales) toma este PN
        trucks_needed = cajas_req / capacidad_truck
        
        # Guardar desglose
        camiones_redondeados = math.ceil(trucks_needed)
        porcentaje_ultimo = ((cajas_req % capacidad_truck) / capacidad_truck) * 100 if cajas_req % capacidad_truck != 0 else 100.0
        
        resumen["Detalle_Por_Num_Parte"].append({
            "Noparte": pn,
            "TME": row['TME'],
            "Tipo_Empaque": row['TIPO DE EMPAQUE'],
            "Demanda_Piezas": int(row['DAILY']),
            "Cajas_Requeridas": int(cajas_req),
            "Cajas_Por_Camion_Max": int(capacidad_truck),
            "Camiones_Llenos": math.floor(trucks_needed),
            "Camion_Extra_Porcentaje": round(porcentaje_ultimo, 2) if camiones_redondeados > math.floor(trucks_needed) else 0.0,
            "Largo_m": row['Largo m'],
            "Ancho_m": row['Ancho m'],
            "Alto_m": row['Alto m'],
            "Altura_plegada_m": row['Altura plegada'],
            "Peso_max_kg": row['Peso max kg']
        })
        
        total_trucks += trucks_needed
        
    resumen["Total_Camiones_Estimados_Exactos"] = round(total_trucks, 2)
    # Se redondea globalmente considerando consolidación de PN en camiones multiparte (heurística de suma de porcentajes espaciales)
    resumen["Total_Camiones_Flota_Requerida"] = math.ceil(total_trucks) 
    
    return resumen

def main(db_path, fecha=None):
    print(f"--- INICIANDO MOTOR DE CUBICAJE VW ---")
    print(f"Base de datos objetivo: {db_path}\n")
    
    print("[1/4] Extrayendo demanda consolidada (JOIN)...")
    df = get_daily_demand(db_path, fecha=fecha)
    
    if df is None or df.empty:
        print("ERROR: No se encontraron registros de demanda aptos. Terminado.")
        return None
        
    print(f"      -> {len(df)} números de parte encontrados con demanda mayor a 0.")
    
    print("[2/4] Calculando cajas requeridas...")
    df = calculate_required_boxes(df)
    
    print("[3/4] Ejecutando algoritmo de Bin Packing 3D...")
    df = calculate_truck_occupancy(df)
    
    print("[4/4] Consolidando plan logístico final...")
    plan = generate_logistics_plan(df)
    
    print("\n================ RESUMEN DE ENVÍO ================")
    print(f"Total de Cajas Requeridas:     {plan.get('Total_Cajas_A_Enviar', 0)}")
    print(f"Flota de Camiones DHL Req.:    {plan.get('Total_Camiones_Flota_Requerida', 0)} unidades (Estimado matemático: {plan.get('Total_Camiones_Estimados_Exactos', 0)})")
    
    if plan.get('Advertencias'):
        print("\nADVERTENCIAS:")
        for w in plan['Advertencias']:
            print(f"- {w}")
            
    print("\n================ DESGLOSE POR TIPO ===============")
    for detalle in plan.get("Detalle_Por_Num_Parte", []):
         print(f"PN: {detalle['Noparte']} | Empaque: {detalle['Tipo_Empaque']} | Cajas: {detalle['Cajas_Requeridas']} (Máx {detalle['Cajas_Por_Camion_Max']}/Camión) -> Camiones: {detalle['Camiones_Llenos']} completos + {detalle['Camion_Extra_Porcentaje']}% extra")
         
    return plan

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Motor de Cubicaje Logístico 3D MVP")
    parser.add_argument("--db", default="logistica_vw.db", help="Ruta de la base de datos SQLite")
    args = parser.parse_args()
    
    # Manejar rutas relativas transparentemente
    base_dir = '/Users/danielfloresrojas/Downloads/VW_R1'
    db_target = args.db if os.path.isabs(args.db) else os.path.join(base_dir, args.db)
    
    main(db_target)
