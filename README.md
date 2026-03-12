# Sistema de Logística JIT "Milk Run" para VW & AKSYS

Este repositorio contiene el sistema integral para sincronizar y gestionar las ventanas de logística Just-in-Time (JIT) en formato "Milk Run" cerrado entre la **Planta VW** y el **Proveedor AKSYS**.

El sistema se compone fundamentalmente de 3 paneles interconectados en tiempo real (WebSockets):
1. **VW Dashboard (Controlador):** Vista global analítica de KPIs, tiempos y alertas automatizadas de cubicaje.
2. **AKSYS Panel (Proveedor):** Panel operativo de piso para que el proveedor reciba empaques vacíos y despache productos terminados a las Naves de VW.
3. **App DHL (Repartidor):** Interfaz móvil (100% responsiva) para que los choferes logísticos sigan su checklist y reporten arribos/salidas.

---

## 🚀 Requisitos Previos (Para otras ingenierías / miembros del equipo)

Necesitas tener instalado en tu computadora:
- **Python 3.9+** (o superior). Verifica abriendo la terminal y corriendo: `python --version` o `python3 --version`.

---

## 💻 Instrucciones de Instalación y Ejecución Local

**Paso 1: Clonar el Repositorio e ingresar a la carpeta**
Abre una terminal/consola y ejecuta:
```bash
git clone https://github.com/danyflores7/VW_Logistics_Sync.git
cd VW_Logistics_Sync
```

**Paso 2: Crear y Activar un Entorno Virtual (Recomendado)**
*(Estando dentro de la carpeta VW_Logistics_Sync)*
```bash
# Para Mac/Linux:
python3 -m venv venv
source venv/bin/activate

# Para Windows (PowerShell/CMD):
python -m venv venv
venv\Scripts\activate
```

**Paso 3: Instalar Dependencias del Proyecto**
Una vez activo el entorno, instala los paquetes de Python:
```bash
pip install -r requirements.txt
```

**Paso 4: Iniciar el Servidor API / WebSockets**
Ejecuta el servidor empleando el comando nativo de Uvicorn/FastAPI:
```bash
uvicorn main_api:app --reload --port 8000
```
> Si te dice "Dirección o puerto ya en uso", usa un puerto distinto como `--port 8080`, o mata el proceso amarrado a ese puerto.

**Paso 5: Acceder a los Paneles en tu Navegador**

Estando el servidor encendido, puedes acceder a todos los ecosistemas del proyecto abriendo estas ligas en Google Chrome, Edge o Safari:

- 🏎️ **Dashboard VW (Controladores):** [http://localhost:8000/frontend/dashboard_vw.html](http://localhost:8000/frontend/dashboard_vw.html)
- 📦 **Panel de AKSYS (Proveedor):** [http://localhost:8000/frontend/panel_proveedor.html](http://localhost:8000/frontend/panel_proveedor.html)
- 🚚 **App de Chofer (DHL):** [http://localhost:8000/frontend/repartidor.html](http://localhost:8000/frontend/repartidor.html)
- 📊 **Reportes de Eficiencia:** [http://localhost:8000/frontend/reportes.html](http://localhost:8000/frontend/reportes.html)
- 📡 **Monitoreo JIT:** [http://localhost:8000/frontend/monitoreo_jit.html](http://localhost:8000/frontend/monitoreo_jit.html)

> **Producción (Render):** Si el servidor está desplegado en Render, sustituye `http://localhost:8000` por la URL de tu servicio (ej. `https://vw-logistics.onrender.com`). Las rutas relativas (`/frontend/...`) funcionan automáticamente.

---

## 📂 ¿Cómo está construido el código?

* **`main_api.py`:** Es el corazón (Backend) construido con **FastAPI**. Aloja todos los endpoints HTTP (REST), gestiona el WebSocket Broadcaster y guarda la máquina de estados lógicos en una base de datos local llamada `logistica_vw.db` usando **SQLite**.
* **`frontend/`:** Directorio que posee todos los archivos HTML (y JS/CSS embebido) para manejar la visualización. Usan una conexión vía WebSockets generada automáticamente para no sobrecargar el servidor (el código del Frontend re-renderea inteligentemente tras recibir los JSON).
* **`data_pipeline.py` & `cubicaje_engine.py`:** Motores pesados (Pandas y Math) que realizan "Data Mining" para interpretar el archivo maestro (Layout de Demanda VW) subido al vuelo desde el Frontend de VW y construyen internamente las ventanas (slots de 06:00, 08:20, etc.) basadas en cálculo volumétrico estricto de bin-packing.
* **`logistica_vw.db`:** Base de datos compacta autogenerada. Solo almacena de forma relacional el Layout (`demanda_besi`) y el Control/Bitácora de todos los envíos ("Entregado", "Transito_Hacia_Prov", etc..). 

## 🔄 El Ciclo Milk-Run

La aplicación sigue fielmente una estricta Máquina de 4 Estados que **no avanzará al siguiente paso si no se concluye el actual**. El ciclo es circular y cada "Viaje Activo" cruza visualmente entre los 3 tableros por los siguientes estados:

1. **`Transito_Hacia_Prov`**: Salida formal desde VW enviando los recipientes cerrados plásticos/metálicos (Vacíos). (Se dispara pulsando `Salida desde VW` en Panel del Repartidor).
2. **`En_Proveedor`**: Check-In del conductor/camión en las cortinas de AKSYS. Entregan material vacío. (Se dispara pulsando `Recibir Vacíos` en el Panel de AKSYS).
3. **`Transito_Hacia_VW`**: AKSYS despide al camión con las cajas _llenas_ de Producto. (Se dispara pulsando `Enviar Llenas` en el Panel de AKSYS).
4. **`Completado`**: El camión hace su Check-In final registrando las PNs entregadas a zona 25 u 84 del complejo VW. (Se dispara pulsando `Completar Viaje` en el Panel del Repartidor).

*(Posterior a esto, el sistema se reinicia habilitándole el panel del siguiente Viaje al chofer).*
