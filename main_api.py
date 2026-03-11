import os
import sqlite3
import json
import math
from contextlib import asynccontextmanager
import shutil
from datetime import datetime
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, File, UploadFile
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
import json

from cubicaje_engine import main as ejecutar_motor_cubicaje
from data_pipeline import procesar_y_guardar_demanda

DB_PATH = '/Users/danielfloresrojas/Downloads/VW_R1/logistica_vw.db'

# === 1. MANEJADOR DE CONEXIONES WEBSOCKETS ===
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        # Convertimos el dict en string JSON plano para transmitirlo
        msg_str = json.dumps(message)
        for connection in self.active_connections:
            try:
                await connection.send_text(msg_str)
            except Exception:
                pass # Si el socket se rompió lo limpiaremos después

manager = ConnectionManager()

# === 2. INICIALIZADOR DE BASE DE DATOS (VIAJES ACTIVOS) ===
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Lógica que corre al arrancar el servidor
    init_db_mock_viajes()
    yield
    # (Opcional) Lógica al apagar el servidor

def init_db_mock_viajes():
    """ 
    Crea la tabla de viajes activos y la llena con viajes reales
    basados en la demanda del Cubicaje Engine y las ventanas de AKSYS.
    """
    if not os.path.exists(DB_PATH):
        return
        
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("DROP TABLE IF EXISTS viajes_activos")
        
        cursor.execute("""
            CREATE TABLE viajes_activos (
                id_viaje INTEGER PRIMARY KEY AUTOINCREMENT,
                noparte TEXT,
                ventana_hora TEXT,
                estado TEXT,
                real_salida_vw TEXT,
                real_llegada_prov TEXT,
                real_salida_prov TEXT,
                real_llegada_vw TEXT,
                cant_vacias_enviadas INTEGER DEFAULT 0,
                cant_vacias_recibidas INTEGER DEFAULT 0,
                cant_llenas_enviadas INTEGER DEFAULT 0,
                cant_llenas_recibidas INTEGER DEFAULT 0,
                minutos_retraso INTEGER DEFAULT 0,
                tme_json TEXT
            )
        """)
        
        ventanas_aksys = ["06:00", "08:20", "10:40", "13:00", "15:35", "18:10", "20:30", "22:50", "01:10", "03:30"]
        plan = ejecutar_motor_cubicaje(DB_PATH)
        if plan and plan.get("Detalle_Por_Num_Parte"):
            partes = plan["Detalle_Por_Num_Parte"]
            mock_data = []
            for i, p in enumerate(partes):
                noparte = p["Noparte"]
                hora = ventanas_aksys[i % len(ventanas_aksys)]
                # Por default iniciamos todos como Pendiente
                estado = "Pendiente"
                mock_data.append((noparte, hora, estado))
            
            cursor.executemany("INSERT INTO viajes_activos (noparte, ventana_hora, estado) VALUES (?, ?, ?)", mock_data)
        
        conn.commit()
        conn.close()
        print(">> Tabla 'viajes_activos' inicializada con datos reales JIT.")
    except Exception as e:
        print(f"Error inicializando viajes mock: {e}")

# Configuración inicial de FastAPI
app = FastAPI(title="Logística VW - MVP API (RealTime)", version="2.0.0", lifespan=lifespan)

# 2. Configurar CORS (Permitir que el frontend JS consulte sin bloqueos)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Montar los estáticos para poder acceder desde localtunnel o red
app.mount("/frontend", StaticFiles(directory=os.path.join(os.path.dirname(DB_PATH), "frontend")), name="frontend")


@app.get("/")
def home():
    """Ruta base para verificar que el servidor está corriendo"""
    return {"mensaje": "API de Logística VW funcionando (WebSockets Habilitados)"}


@app.get("/api/resumen-logistico")
def get_resumen_logistico():
    # ... Lógica nativa de obtención ...
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=404, detail="Base de datos SQLite no encontrada.")
        
    try:
        resultado_plan = ejecutar_motor_cubicaje(DB_PATH)
        if resultado_plan is None or "error" in resultado_plan:
             raise HTTPException(status_code=400, detail="Error o falta de datos procesando la demanda.")
        return resultado_plan
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno del motor: {str(e)}")


@app.get("/api/proveedor/piezas-hoy")
def get_total_piezas_hoy():
    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=404, detail="Base de datos no encontrada.")
        
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT SUM(CAST(DAILY AS REAL)) FROM demanda_besi WHERE DAILY > 0")
        total = cursor.fetchone()[0]
        conn.close()
        return {
            "fecha_reporte": "Hoy",
            "total_piezas_demandadas": int(total) if total is not None else 0
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando SQL: {str(e)}")

# === 3. ENDPOINTS DE TIEMPO REAL === 

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                import json
                payload = json.loads(data)
                if payload.get("tipo") == "gps_update":
                    await manager.broadcast(payload)
            except Exception:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/api/viajes-activos")
def read_viajes_activos():
    """ Devuelve el listado actual SQL de los viajes con todo el historico de estado """
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM viajes_activos")
        filas = cursor.fetchall()
        conn.close()
        
        viajes = [dict(f) for f in filas]
        return viajes
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error SQL: {e}")

class ActualizacionEstado(BaseModel):
    id_viaje: int
    nuevo_estado: str
    cantidad: int = 0
    tme_dict: Dict[str, int] = None

@app.post("/api/actualizar-estado")
async def actualizar_estado_viaje(payload: ActualizacionEstado):
    """
    1. Actualiza SQLite (con columnas de cantidades y timestamps)
    2. Emite Broadcast vía WS al Frontend
    """
    valid_status = ["Pendiente", "Transito_Hacia_Prov", "En_Proveedor", "Transito_Hacia_VW", "Completado"]
    if payload.nuevo_estado not in valid_status and payload.nuevo_estado not in ["En_Transito", "Entregado"]: # Soporte legado
        raise HTTPException(status_code=400, detail="Estado Invalido")
        
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        now_str = datetime.now().isoformat()
        
        updates = ["estado = ?"]
        params = [payload.nuevo_estado]
        
        if payload.nuevo_estado == "Transito_Hacia_Prov":
            updates.extend(["real_salida_vw = ?", "cant_vacias_enviadas = ?"])
            params.extend([now_str, payload.cantidad])
        elif payload.nuevo_estado == "En_Proveedor":
            updates.extend(["real_llegada_prov = ?", "cant_vacias_recibidas = ?"])
            params.extend([now_str, payload.cantidad])
        elif payload.nuevo_estado == "Transito_Hacia_VW":
            updates.extend(["real_salida_prov = ?", "cant_llenas_enviadas = ?"])
            params.extend([now_str, payload.cantidad])
        elif payload.nuevo_estado == "Completado" or payload.nuevo_estado == "Entregado":
            payload.nuevo_estado = "Completado" # Forzar nombre nuevo
            cursor.execute("SELECT ventana_hora FROM viajes_activos WHERE id_viaje = ?", (payload.id_viaje,))
            row_vh = cursor.fetchone()
            retraso = 0
            if row_vh:
                vh = row_vh[0] # Ej: "10:10"
                ahora = datetime.now()
                try:
                    hora_planeada = ahora.replace(hour=int(vh.split(":")[0]), minute=int(vh.split(":")[1]), second=0, microsecond=0)
                    diff = (ahora - hora_planeada).total_seconds() / 60
                    retraso = int(diff)
                except:
                    retraso = 0
            
            updates.extend(["real_llegada_vw = ?", "cant_llenas_recibidas = ?", "minutos_retraso = ?"])
            params.extend([now_str, payload.cantidad, retraso])
            
        if payload.tme_dict is not None:
            updates.extend(["tme_json = ?"])
            params.extend([json.dumps(payload.tme_dict)])
            
        params.append(payload.id_viaje)
        query = f"UPDATE viajes_activos SET {', '.join(updates)} WHERE id_viaje = ?"
        
        cursor.execute(query, params)
        
        # Recuperar estado completo para enviar por WS
        conn.row_factory = sqlite3.Row
        c2 = conn.cursor()
        c2.execute("SELECT * FROM viajes_activos WHERE id_viaje = ?", (payload.id_viaje,))
        row_updated = c2.fetchone()
        
        conn.commit()
        conn.close()
        
        dict_row = dict(row_updated) if row_updated else {}
        
        # ¡Hacemos BROADCAST a todas las pantallas VW/AKSYS/DHL conectadas!
        evento = {
            "tipo": "actualizacion_viaje",
            "id_viaje": payload.id_viaje,
            "noparte": dict_row.get("noparte", ""),
            "estado": payload.nuevo_estado,
            "viaje": dict_row
        }
        await manager.broadcast(evento)
        
        return {"success": True, "message": "Estado actualizado y broadcasted."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error actualizando estado: {e}")

@app.post("/api/upload/demanda")
async def upload_demanda_file(file: UploadFile = File(...)):
    """
    1. Recibe el archivo subido desde el frontend
    2. Lo guarda de forma local (sobre-escribiendo)
    3. Ejecuta el ETL pipeline de data_pipeline.py para poblar BD
    4. Emite Broadcast de WebSocket reportando el cambio
    """
    try:
        # 1. Guardar el archivo temporalmente
        file_location = f"/Users/danielfloresrojas/Downloads/VW_R1/{file.filename}"
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        print(f"Archivo recibido y guardado en: {file_location}")
            
        # 2. Ejecutar la integración de datos hacia SQLite
        rows_affected = procesar_y_guardar_demanda(file_path=file_location, db_name=DB_PATH)
        
        if rows_affected > 0:
            # 3. Notificar a todo mundo a través de WebSockets
            await manager.broadcast({"tipo": "nueva_demanda", "mensaje": "Demanda JIT actualizada."})
            return {"success": True, "message": f"Demanda actualizada con {rows_affected} registros.", "filename": file.filename}
        else:
            raise HTTPException(status_code=400, detail="El archivo se guardó, pero no contenía datos procesables o falló el ETL.")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno durante la carga de demanda: {str(e)}")

# === 4. ENDPOINTS PARA PANELES ESPECÍFICOS ===

@app.get("/api/proveedor/ventanas")
def get_proveedor_ventanas(fecha: str = None):
    """ Devuelve la lista de ventanas JIT con algoritmo Heijunka de nivelación de carga y bin packing de ida """
    try:
        # 1. Ventanas estáticas base
        ventanas_n25 = ["06:00", "08:20", "10:40", "13:00", "18:10", "20:30", "22:50", "01:10", "03:30"]
        ventanas_n84 = ["15:35"]
        
        # 2. Preparar el diccionario de respuesta por cada ventana
        ventanas_dict = {}
        for h in ventanas_n25 + ventanas_n84:
            ventanas_dict[h] = {
                "id_viaje": len(ventanas_dict) + 1,
                "hora_ventana": h,
                "zona_logistica": "Nave 25" if h in ventanas_n25 else "Nave 84",
                "ocupacion_porcentaje": 0.0,
                "fraccion_fisica_camiones": 0.0,
                "volumen_cajas_m3": 0.0,
                "sobrecupo": False,
                "estado": "Pendiente",
                "partes": []
            }
            
        # 2.5 Leer el estado dinámico y real desde base de datos SQLite solo si es HOY
        from datetime import datetime
        hoy_str = datetime.now().strftime('%d/%m/%Y')
        is_today = (not fecha or fecha == "DAILY" or fecha == hoy_str)
        
        if is_today:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT id_viaje, ventana_hora, estado FROM viajes_activos")
                for row in cursor.fetchall():
                    hora = row["ventana_hora"]
                    if hora in ventanas_dict:
                        # Sobrescribir estado e ID para que coincidan con la DB
                        ventanas_dict[hora]["estado"] = row["estado"]
                        ventanas_dict[hora]["id_viaje"] = row["id_viaje"]
            except Exception:
                pass # Si falla continua con los mocks
            finally:
                conn.close()
                
        # 3. Leer motor de cubicaje
        plan = ejecutar_motor_cubicaje(DB_PATH, fecha=fecha)
        detalles_pn = plan.get("Detalle_Por_Num_Parte", []) if plan else []
        
        TRUCK_LENGTH = 6.5
        TRUCK_WIDTH = 2.5
        TRUCK_HEIGHT = 2.4
        TRUCK_MAX_WEIGHT_KG = 5000
        TRUCK_VOLUMEN_M3 = TRUCK_LENGTH * TRUCK_WIDTH * TRUCK_HEIGHT
        
        piezas_hoy = 0
        empaques_vacios = 0
        
        for p in detalles_pn:
            noparte = p.get("Noparte")
            tme = p.get("Tipo_Empaque")
            tmg_zona = p.get("TME") # Este es el TME que diferencia la zona (17A o 2GM)
            cajas_req = p.get("Cajas_Requeridas", 0)
            piezas = p.get("Demanda_Piezas", 0)
            
            # Dimensiones
            l = float(p.get("Largo_m", 0))
            w = float(p.get("Ancho_m", 0))
            h_llena = float(p.get("Alto_m", 0))
            peso_kg = float(p.get("Peso_max_kg", 0))
            
            if l <= 0 or w <= 0 or h_llena <= 0:
                continue
                
            # Cálculo de Bin Packing Llenas
            cajas_base = math.floor(TRUCK_LENGTH / l) * math.floor(TRUCK_WIDTH / w)
            cajas_alto = math.floor(TRUCK_HEIGHT / h_llena)
            cap_max_volumen = cajas_base * cajas_alto
            cap_max_peso = math.floor(TRUCK_MAX_WEIGHT_KG / peso_kg) if peso_kg > 0 else cap_max_volumen
            cap_max_llenas = min(cap_max_volumen, cap_max_peso)
            
            if cap_max_llenas <= 0:
                continue
                
            piezas_hoy += piezas
            empaques_vacios += cajas_req
            
            volumen_por_caja_m3 = l * w * h_llena

            # Nave 84 (TMG_ZONA = 2GM)
            if tmg_zona == "2GM":
                hora_asignada = ventanas_n84[0]
                ventanas_dict[hora_asignada]["partes"].append({
                    "noparte": noparte,
                    "tipo_empaque": tme,
                    "vacias_recibir": cajas_req,
                    "llenas_enviar": cajas_req,
                    "l_m": l,
                    "w_m": w,
                    "h_m": h_llena
                })
                # Sumar requerimiento físico de camiones y volumen
                ventanas_dict[hora_asignada]["fraccion_fisica_camiones"] += cajas_req / cap_max_llenas
                ventanas_dict[hora_asignada]["volumen_cajas_m3"] += cajas_req * volumen_por_caja_m3
            else:
                # Nave 25 Heijunka (dividir en 9 ventanas)
                cajas_por_viaje = math.floor(cajas_req / len(ventanas_n25))
                cajas_sobrantes = cajas_req % len(ventanas_n25)
                
                for idx, h in enumerate(ventanas_n25):
                    asignadas_viaje = cajas_por_viaje + (1 if idx < cajas_sobrantes else 0)
                    if asignadas_viaje > 0:
                        ventanas_dict[h]["partes"].append({
                            "noparte": noparte,
                            "tipo_empaque": tme,
                            "vacias_recibir": asignadas_viaje,
                            "llenas_enviar": asignadas_viaje,
                            "l_m": l,
                            "w_m": w,
                            "h_m": h_llena
                        })
                        ventanas_dict[h]["fraccion_fisica_camiones"] += asignadas_viaje / cap_max_llenas
                        ventanas_dict[h]["volumen_cajas_m3"] += asignadas_viaje * volumen_por_caja_m3

        # 4. Validar camiones requeridos y empaquetar lista
        ventanas_lista = []
        # Orden cronológico según el PDF (6am a 3am)
        orden_ventanas = ["06:00", "08:20", "10:40", "13:00", "15:35", "18:10", "20:30", "22:50", "01:10", "03:30"]
        for h in orden_ventanas:
            v_data = ventanas_dict[h]
            fraccion_fisica = v_data.pop("fraccion_fisica_camiones", 0)
            volumen_cajas = v_data.pop("volumen_cajas_m3", 0)
            
            # Calcular camiones físicos requeridos basados estrictamente en el volumen puro (como el simulador)
            camiones_req = math.ceil(volumen_cajas / TRUCK_VOLUMEN_M3) if volumen_cajas > 0 else 1
            camiones_req = max(1, camiones_req)
            
            # Cálculo Real Volumétrico
            volumen_total_camiones_usados = camiones_req * TRUCK_VOLUMEN_M3
            porcentaje_volumetrico = (volumen_cajas / volumen_total_camiones_usados) * 100 if volumen_total_camiones_usados > 0 else 0
            
            v_data["sobrecupo"] = False 
            v_data["ocupacion_porcentaje"] = round(porcentaje_volumetrico, 1)
            ventanas_lista.append(v_data)

        # Retornar KPIs globales
        return {
            "ventanas": ventanas_lista,
            "kpi_piezas_hoy": piezas_hoy,
            "kpi_empaques_vacios": empaques_vacios
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error generando ventanas: {e}")

@app.get("/api/dhl/retornos-vacios")
def get_dhl_retornos_vacios(fecha: str = None):
    """ Devuelve el nivelado logístico de empaques vacíos devueltos al proveedor (10 camiones DHL programados desde VW) """
    try:
        plan = ejecutar_motor_cubicaje(DB_PATH, fecha=fecha)
        if not plan:
            return {"ventanas": []}

        detalles_pn = plan.get("Detalle_Por_Num_Parte", [])
        
        # Horarios exactos de 'Salida VW' del ciclo para retornos de vacíos (Para usarse en la ventana siguiente)
        ventanas_dhl_n25 = ["05:20", "07:50", "10:10", "12:30", "17:40", "20:00", "22:20", "00:40", "03:00"]
        ventanas_dhl_n84 = ["14:50"] # El viaje de la 15:35 usa los vacíos que salen de VW a las 14:50
        ventanas_dict = {
            h: {
                "id_viaje": idx + 100,
                "hora_ventana": h,
                "ocupacion_porcentaje": 0.0,
                "sobrecupo": False,
                "estado": "Pendiente",
                "partes": []
            } for idx, h in enumerate(ventanas_dhl_n25 + ventanas_dhl_n84)
        }
        
        TRUCK_LENGTH = 6.5
        TRUCK_WIDTH = 2.5
        TRUCK_HEIGHT = 2.4
        TRUCK_MAX_WEIGHT_KG = 5000
        
        for p in detalles_pn:
            noparte = p.get("Noparte")
            tme = p.get("Tipo_Empaque")
            tmg_zona = p.get("TME") # TME define si es 2GM (Nave 84) u otros (Nave 25)
            cajas_req = p.get("Cajas_Requeridas", 0)
            
            # Dimensiones
            l = float(p.get("Largo_m", 0))
            w = float(p.get("Ancho_m", 0))
            h_plegada = float(p.get("Altura_plegada_m", p.get("Alto_m", 0))) # Fallback a alto normal
            peso_kg = float(p.get("Peso_max_kg", 0))
            
            if l <= 0 or w <= 0 or h_plegada <= 0 or cajas_req <= 0:
                continue
                
            # Cálculo de Bin Packing PLEGADAS
            cajas_base = math.floor(TRUCK_LENGTH / l) * math.floor(TRUCK_WIDTH / w)
            cajas_alto = math.floor(TRUCK_HEIGHT / h_plegada)
            cap_max_volumen = cajas_base * cajas_alto
            cap_max_peso = math.floor(TRUCK_MAX_WEIGHT_KG / peso_kg) if peso_kg > 0 else cap_max_volumen
            cap_max_plegadas = min(cap_max_volumen, cap_max_peso)
            
            if cap_max_plegadas <= 0:
                continue

            # Nivelado exacto acorde a salidas llenas
            if tmg_zona == "2GM":
                hora_asignada = ventanas_dhl_n84[0]
                ventanas_dict[hora_asignada]["partes"].append({
                    "noparte": noparte,
                    "tipo_empaque": tme,
                    "vacias_retornar": cajas_req,
                    "l_m": l,
                    "w_m": w,
                    "h_m": h_plegada
                })
                fraccion = cajas_req / cap_max_plegadas
                ventanas_dict[hora_asignada]["ocupacion_porcentaje"] += fraccion * 100
            else:
                # Heijunka de vacías NAVE 25 (dividir en 9 ventanas correspondientes)
                cajas_por_viaje = math.floor(cajas_req / len(ventanas_dhl_n25))
                cajas_sobrantes = cajas_req % len(ventanas_dhl_n25)
                
                for idx, h in enumerate(ventanas_dhl_n25):
                    asignadas_viaje = cajas_por_viaje + (1 if idx < cajas_sobrantes else 0)
                    if asignadas_viaje > 0:
                        ventanas_dict[h]["partes"].append({
                            "noparte": noparte,
                            "tipo_empaque": tme,
                            "vacias_retornar": asignadas_viaje, # En dhl las llamamos retornar
                            "l_m": l,
                            "w_m": w,
                            "h_m": h_plegada
                        })
                        fraccion = asignadas_viaje / cap_max_plegadas
                        ventanas_dict[h]["ocupacion_porcentaje"] += fraccion * 100

        # Validar sobrecupos y aplicar el orden natural de retornos de vacíos
        ventanas_lista = []
        orden_retornos = ["07:50", "10:10", "12:30", "14:50", "17:40", "20:00", "22:20", "00:40", "03:00", "05:20"]
        for h in orden_retornos:
            v_data = ventanas_dict[h]
            if v_data["ocupacion_porcentaje"] > 100:
                v_data["sobrecupo"] = True
            v_data["ocupacion_porcentaje"] = round(v_data["ocupacion_porcentaje"], 1)
            ventanas_lista.append(v_data)

        return {"ventanas": ventanas_lista}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error generando retornos: {e}")

@app.get("/api/repartidor/viaje-actual")
def get_repartidor_viaje_actual():
    """ Devuelve el viaje JIT actual del chofer DHL – fuente de verdad: motor de ventanas """
    try:
        # 1. Leer estados reales desde SQLite (solo para sobrescribir si hay cambio manual)
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id_viaje, ventana_hora, estado FROM viajes_activos ORDER BY id_viaje ASC")
            viajes_db = cursor.fetchall()
        except Exception:
            viajes_db = []
        conn.close()
        
        estado_por_ventana = {row["ventana_hora"]: {"id": row["id_viaje"], "estado": row["estado"]} for row in viajes_db}
        
        # 2. Obtener ventanas ricas del motor (igual que panel_proveedor.html)
        proveedor_data = get_proveedor_ventanas(fecha=None)
        ventanas = proveedor_data.get("ventanas", [])
        
        if not ventanas:
            return {"mensaje": "No hay ventanas configuradas para hoy."}
        
        total_ventanas = len(ventanas)
        entregadas = 0
        viaje_actual = None
        
        for v in ventanas:
            hora = v["hora_ventana"]
            info_db = estado_por_ventana.get(hora, {})
            # Estado en DB gana si existe; si no, usa el del motor
            estado_real = info_db.get("estado", v.get("estado", "Pendiente"))
            
            if estado_real in ("Completado", "Entregado"):
                entregadas += 1
            elif viaje_actual is None:  # primera no-entregada = viaje activo
                viaje_actual = {
                    "id_viaje": info_db.get("id", v.get("id_viaje", 0)),
                    "hora_ventana": hora,
                    "zona_logistica": v["zona_logistica"],
                    "ocupacion_porcentaje": v["ocupacion_porcentaje"],
                    "estado": estado_real,
                    "partes": v.get("partes", []),
                    "cantidad_cajas": sum(p.get("llenas_enviar", 0) for p in v.get("partes", [])),
                    "porcentaje_cubicaje": v["ocupacion_porcentaje"],
                    "hora_salida": hora,
                    "destino": v["zona_logistica"]
                }
        
        if not viaje_actual:
            return {
                "mensaje": "!Todas las entregas del dia completadas!",
                "progreso": {"entregadas": entregadas, "total": total_ventanas},
                "completado": True
            }  # 3. Construir lista de partes para checklist del chofer
        partes_lista = []
        total_cajas = 0
        tipos_empaque = set()
        for p in viaje_actual["partes"]:
            cajas = p.get("llenas_enviar", 0)
            total_cajas += cajas
            tipos_empaque.add(p.get("tipo_empaque", ""))
            partes_lista.append({
                "noparte": p["noparte"],
                "tipo_empaque": p["tipo_empaque"],
                "cajas": cajas
            })
        
        return {
            "id_viaje": viaje_actual["id_viaje"],
            "hora_salida": viaje_actual["hora_ventana"],
            "destino": viaje_actual["zona_logistica"],
            "tipo_empaque": ", ".join(sorted(tipos_empaque)) if tipos_empaque else "N/A",
            "cantidad_cajas": total_cajas,
            "porcentaje_cubicaje": round(viaje_actual["ocupacion_porcentaje"], 1),
            "estado": viaje_actual["estado"],
            "partes": partes_lista,
            "progreso": {"entregadas": entregadas, "total": total_ventanas}
        }
    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error obteniendo viaje actual: {e}")

@app.get("/api/vw/dashboard-data")
def get_vw_dashboard_data(fecha: str = None):
    """ Construye la data consolidada para el Dashboard Analítico (Controlador VW) """
    try:
        # Prevent empty string from breaking logic
        if fecha == "":
            fecha = None
            
        # 1. Obtener la data maestra del motor de cubicaje para cálculos teóricos
        plan = ejecutar_motor_cubicaje(DB_PATH, fecha=fecha)
        detalles_pn = plan.get("Detalle_Por_Num_Parte", []) if plan else []
        total_empaques_hoy = plan.get("Total_Cajas_A_Enviar", 0) if plan else 0
        camiones_requeridos = plan.get("Total_Camiones_Flota_Requerida", 0) if plan else 0
        
        # 2. Obtener estado real de los viajes desde SQLite
        from datetime import datetime
        hoy_str = datetime.now().strftime('%d/%m/%Y')
        is_today = (not fecha or fecha == "DAILY" or fecha == hoy_str)
        
        viajes_db = []
        if is_today:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM viajes_activos ORDER BY ventana_hora ASC")
            viajes_db = cursor.fetchall()
            conn.close()

        # Estructuras de respuesta principal
        info_por_ventana = {}
        for row in viajes_db:
            info_por_ventana[row["ventana_hora"]] = dict(row)
            
        proveedor_data = get_proveedor_ventanas(fecha=fecha)
        lista_ventanas = proveedor_data.get("ventanas", [])
        
        viajes_vivo = []
        alertas = []

        if camiones_requeridos > 10:
            alertas.append({
                "id": f"alerta_predict_{len(alertas)+1}",
                "nivel": "critico",
                "titulo": "ALERTA PREDICTIVA: Riesgo de Desabasto",
                "desc_corta": f"La demanda requerirá {camiones_requeridos} camiones, superando la capacidad máxima de 10 viajes.",
                "accion": "Ver en Simulador",
                "tiempo": "Ahora"
            })
            
        grafica_jit = []
        total_entregados = 0
        ventanas_activas = 0
        ocupacion_total_calculada = 0
        import math
        import json
        total_cajas_reales_dia = 0
        
        # New globals for TME KPIs
        reales_dia_por_tme = {}
        esperadas_dia_por_tme = {}
            
        for ventana in lista_ventanas:
            h = ventana["hora_ventana"]
            porcentaje_avg = ventana["ocupacion_porcentaje"]
            
            db_info = info_por_ventana.get(h, {})
            # El estado en SQLite puede predominar, de lo contrario usamos el default del mock
            estado = db_info.get("estado", ventana["estado"])
            
            # Cálculo de KPI del Viaje y TME breakdown
            cajas_esperadas = sum(p.get("llenas_enviar", 0) for p in ventana.get("partes", []))
            cajas_reales_viaje = db_info.get("cant_llenas_recibidas") or db_info.get("cant_llenas_enviadas") or 0
            
            cajas_vacias_esperadas = sum(p.get("vacias_recibir", 0) for p in ventana.get("partes", []))
            cajas_vacias_reales_viaje = db_info.get("cant_vacias_recibidas") or db_info.get("cant_vacias_enviadas") or 0
            
            kpi_viaje = 0.0
            kpi_viaje_vacias = 0.0
            
            # Dictionary of real TME sums sent by repartidor
            tme_dict_real = {}
            if db_info.get("tme_json"):
                try: tme_dict_real = json.loads(db_info["tme_json"])
                except: pass
            
            # Calculate expected TME sums for this trip
            tme_esperado = {}
            for p in ventana.get("partes", []):
                tipo = p.get("tipo_empaque", "N/A")
                tme_esperado[tipo] = tme_esperado.get(tipo, 0) + p.get("llenas_enviar", 0)
                
            kpi_viaje_dict = {}
            
            for tme, esp in tme_esperado.items():
                esperadas_dia_por_tme[tme] = esperadas_dia_por_tme.get(tme, 0) + esp
                
            if estado in ('Completado', 'Entregado', 'Transito_Hacia_VW', 'En_Proveedor'):
                kpi_viaje = round((cajas_reales_viaje / cajas_esperadas) * 100, 1) if cajas_esperadas > 0 else 100.0
                kpi_viaje_vacias = round((cajas_vacias_reales_viaje / cajas_vacias_esperadas) * 100, 1) if cajas_vacias_esperadas > 0 else 100.0
                
                total_cajas_reales_dia += cajas_reales_viaje
                
                for tme, esp in tme_esperado.items():
                    # Fallback if tme_json is missing but we have real boxes
                    real = tme_dict_real.get(tme, esp if cajas_reales_viaje > 0 else 0)
                    pct = round((real / esp) * 100, 1) if esp > 0 else 100.0
                    kpi_viaje_dict[tme] = {"real": real, "esperado": esp, "porcentaje": pct}
                    reales_dia_por_tme[tme] = reales_dia_por_tme.get(tme, 0) + real
            else:
                for tme, esp in tme_esperado.items():
                    kpi_viaje_dict[tme] = {"esperado": esp, "real": 0, "porcentaje": 0}
            
            # Gráfica JIT
            tiene_datos = estado in ('Completado', 'Entregado', 'Transito_Hacia_VW', 'En_Proveedor')
            
            mapa_horas_vacias = {
                "06:00": "07:50", "08:20": "10:10", "10:40": "12:30", "13:00": "14:50", "15:35": "17:40",
                "18:10": "20:00", "20:30": "22:20", "22:50": "00:40", "01:10": "03:00", "03:30": "05:20"
            }
            
            grafica_jit.append({
                "hora": h,
                "hora_vacias": mapa_horas_vacias.get(h, h),
                "porcentaje_meta": 100.0,
                "porcentaje_real_llenas": float(kpi_viaje) if tiene_datos else 0.0,
                "porcentaje_real_vacias": float(kpi_viaje_vacias) if tiene_datos else 0.0,
                "past": estado in ('Completado', 'Entregado'),
                "kpi_viaje": kpi_viaje,
                "kpi_viaje_vacias": kpi_viaje_vacias,
                "tiene_datos": tiene_datos
            })
            
            # KPIs Iteración
            if len(ventana["partes"]) > 0:
                ocupacion_total_calculada += porcentaje_avg
                ventanas_activas += 1
                
            if estado in ('Completado', 'Entregado'):
                total_entregados += 1
                
            # Alertas: Retrasos Críticos
            if estado == 'Retraso':
                alertas.append({
                    "nivel": "critico",
                    "titulo": f"Retraso Crítico de Envío JIT",
                    "desc_corta": f"AKSYS {ventana['zona_logistica']} - Ventana {h}",
                    "accion": "Rastrear",
                    "tiempo": "Justo ahora"
                })
                
            # Alertas: Desabasto Crítico JIT (KPI <= 50%)
            if estado in ('Completado', 'Entregado', 'Transito_Hacia_VW', 'En_Proveedor') and kpi_viaje <= 50 and cajas_esperadas > 0:
                alertas.append({
                    "nivel": "critico",
                    "titulo": f"Desabasto Severo ({kpi_viaje}%)",
                    "desc_corta": f"Merma logística en AKSYS {ventana['zona_logistica']} - Ventana {h}",
                    "accion": "Activar Protocolo",
                    "tiempo": "Alerta Activa"
                })
                
            # Alerta de ocupación
            if porcentaje_avg < 50 and len(ventana["partes"]) > 0 and estado not in ('Completado', 'Entregado'):
                alertas.append({
                    "nivel": "advertencia",
                    "titulo": f"Ocupación Logística Subóptima ({int(porcentaje_avg)}%)",
                    "desc_corta": f"Ventana de las {h} lleva camiones ineficientes. Riesgo TCO.",
                    "accion": "Consolidar",
                    "tiempo": "Hace 2 min"
                })

            # Añadir filas para la tabla en vivo (adjuntar kpi detail directly for rendering row groups if needed, though we only render at window level usually)
            kpi_obj = {"global": kpi_viaje, "tmes": kpi_viaje_dict}
            for p in ventana["partes"]:
                viajes_vivo.append({
                    "hora": h,
                    "proveedor": f"AKSYS {ventana['zona_logistica']}",
                    "noparte": p["noparte"],
                    "empaques": p["llenas_enviar"],
                    "estado": estado,
                    "zona_tme": f"{ventana['zona_logistica']} (TME: {p['tipo_empaque']})",
                    "kpi_viaje": kpi_viaje,
                    "kpi_detail": kpi_obj
                })

        # --- DATA MINING / PREDICCIÓN DE RIESGO DE DESABASTO ---
        import datetime
        conn_prd = sqlite3.connect(DB_PATH)
        cursor_prd = conn_prd.cursor()
        cursor_prd.execute("PRAGMA table_info(demanda_besi)")
        columnas_db = [x[1] for x in cursor_prd.fetchall()]
        conn_prd.close()
        
        # Determinar fecha base
        if fecha and fecha != "DAILY":
            try:
                base_date = datetime.datetime.strptime(fecha, "%d/%m/%Y")
            except Exception:
                base_date = datetime.datetime.now()
        else:
            base_date = datetime.datetime.now()
            
        for i in range(1, 4):
            next_date = base_date + datetime.timedelta(days=i)
            next_date_str = next_date.strftime("%d/%m/%Y")
            
            if next_date_str in columnas_db:
                # Ejecutar motor predictivo para los siguientes días
                plan_futuro = ejecutar_motor_cubicaje(DB_PATH, fecha=next_date_str)
                if plan_futuro:
                    camiones_necesarios = plan_futuro.get("Total_Camiones_Flota_Requerida", 0)
                    if camiones_necesarios > 10:  # Regla del negocio, max 10 camiones
                        alertas.insert(0, {
                            "nivel": "critico",
                            "titulo": f"Predicción de Desabasto ({next_date_str})",
                            "desc_corta": f"Demanda logística excede capacidad de transporte local (Req: {camiones_necesarios} > Máx: 10 cam.)",
                            "accion": "Planear Transporte",
                            "tiempo": "Proyección IA"
                        })
        # 5. KPIs Finales
        ocupacion_dhl_kpi = (ocupacion_total_calculada / ventanas_activas) if ventanas_activas > 0 else 0
        cumplimiento_global_kpi = (total_cajas_reales_dia / total_empaques_hoy * 100) if total_empaques_hoy > 0 else 100.0
        
        kpi_dia_por_tme = {}
        for tme, esp in esperadas_dia_por_tme.items():
            real = reales_dia_por_tme.get(tme, 0)
            pct = round((real / esp) * 100, 1) if esp > 0 else 0.0
            kpi_dia_por_tme[tme] = {"real": real, "esperado": esp, "porcentaje": pct}
            
        # Calcular el total de piezas sumando la demanda
        total_piezas_hoy = sum(int(x.get("Demanda_Piezas", 0)) for x in detalles_pn)
        
        return {
            "kpis": {
                "total_empaques": total_empaques_hoy,
                "total_piezas": total_piezas_hoy,
                "ocupacion_dhl": round(ocupacion_dhl_kpi, 1),
                "cumplimiento_global": round(cumplimiento_global_kpi, 1),
                "por_tme": kpi_dia_por_tme
            },
            "alertas": alertas,
            "grafica_jit": grafica_jit,
            "viajes_vivo": viajes_vivo
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error cargando dashboard datos: {e}")
