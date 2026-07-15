# Panel de Ventas · Gaven

Dos piezas separadas:

- **data_pipeline.py** — se conecta al API de Chess, trae y procesa las ventas del año
  y guarda el resultado en `data/ventas_actualizadas.parquet` (+ `data/metadata.json`
  con la fecha/hora de última actualización). No depende de Streamlit.
- **app.py** — panel de Streamlit que SOLO lee ese parquet. No llama al API.

## 1) Probar local
```
pip install -r requirements.txt
```
Credenciales (elegí una opción):
- Variables de entorno: `CHESS_BASE_URL`, `CHESS_USUARIO`, `CHESS_PASSWORD`, o
- `.streamlit/secrets.toml` (copiá `secrets.toml.example`), sección `[chess]`.

Generá los datos y levantá el panel:
```
python data_pipeline.py        # crea data/ventas_actualizadas.parquet
streamlit run app.py
```
Si abrís la app sin haber corrido el pipeline, te avisa que falta el archivo.

## 2) Programar el pipeline (2 veces por día)

**Linux/Mac (cron)** — `crontab -e`:
```
0 8,20 * * * cd /ruta/al/proyecto && /ruta/al/venv/bin/python data_pipeline.py >> pipeline.log 2>&1
```

**Windows (Programador de tareas)**: nueva tarea -> Disparador diario 08:00
(y otra 20:00) -> Acción: `python.exe` con argumento `data_pipeline.py` y
"Iniciar en" = la carpeta del proyecto.

## 3) Período en la app
Selector con **todos los meses de 2026** que estén en el parquet:
- El mes **en curso** va del 1° hasta hoy (con proyección a fin de mes).
- Los meses **cerrados** van del 1° al último día del mes.

El pipeline mantiene el detalle de todo el año por **upsert mensual**: en cada
corrida re-trae solo el mes actual + el anterior, y si detecta meses de 2026
que faltan en el parquet los baja UNA sola vez (auto-backfill). Los meses
cerrados ya guardados no se vuelven a pedir, así la corrida normal sigue
siendo rápida. La primera corrida después de este cambio tarda más (baja
enero → hoy).
