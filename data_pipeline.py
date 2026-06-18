"""
data_pipeline.py
----------------
ETL: se conecta al API de Chess, trae las ventas del MES ANTERIOR (completo) y
del MES ACTUAL (del día 1 hasta hoy), las limpia/procesa y guarda el resultado
final en data/ventas_actualizadas.parquet.

Importante: la API NO responde bien a rangos largos (varios meses de una sola
vez devuelve 0 filas). Por eso se consulta MES POR MES y se concatena. Así los
meses nunca se mezclan entre sí.

NO depende de Streamlit. Se ejecuta solo:

    python data_pipeline.py

Pensado para correr 2 veces por día (cron / Programador de tareas), por ej.
08:00 y 20:00. La app solo lee el parquet que deja este script.

Credenciales (en este orden):
  1) Variables de entorno: CHESS_BASE_URL, CHESS_USUARIO, CHESS_PASSWORD
  2) Archivo .streamlit/secrets.toml (mismo que usa la app), sección [chess]
"""

import os
import json
import datetime as dt

import requests
import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Rutas y constantes
# ---------------------------------------------------------------------------

ANIO = 2026  # solo se analizan datos de este año

DATA_DIR = "data"
PARQUET_PATH = os.path.join(DATA_DIR, "ventas_actualizadas.parquet")
META_PATH = os.path.join(DATA_DIR, "metadata.json")

BASE_URL_DEFAULT = "https://lachichiessa.chesserp.com/AR683/web/api/chess/v1"
USUARIO_DEFAULT = "DinamicaApis"

COLUMNAS_IMPORTANTES = [
    "dsEmpresa", "dsDocumento", "nrodoc", "anulado", "fechaComprobate",
    "idCliente", "nombreCliente", "dsLocalidad", "dsProvincia",
    "idVendedor", "dsVendedor",
    "idCanalMkt", "dsCanalMkt", "idSubcanalMkt", "dsSubcanalMKT",
    "idArticulo", "dsArticulo", "dsTipoMercaderia", "proveedor",
    "cantidadesTotal", "peso", "pesoTotal", "unimedtotal", "unimedcargo",
    "precioUnitarioNeto", "subtotalNeto", "subtotalFinal", "preciocomprant",
]

COLUMNAS_NUMERICAS = [
    "cantidadesTotal", "peso", "pesoTotal", "unimedtotal", "unimedcargo",
    "precioUnitarioNeto", "subtotalNeto", "subtotalFinal", "preciocomprant",
]

CLIENTES_EXCLUIR = [194, 762, 1043, 1046, 1050, 1054]

MAPA_REGION = {
    "CIUDAD AUTONOMA BUENOS AIRES": "CABA",
    "BELLA VISTA": "SAN MIGUEL", "MUÑIZ": "SAN MIGUEL", "SAN MIGUEL": "SAN MIGUEL",
    "JOSE CLEMENTE PAZ": "JOSE C PAZ",
    "GRAND BOURG": "MALVINAS", "LOS POLVORINES": "MALVINAS", "PABLO NOGUES": "MALVINAS",
    "TORTUGUITAS": "MALVINAS", "VILLA DE MAYO": "MALVINAS", "INGENIERO ADOLFO SOURDEAUX": "MALVINAS",
    "DEL VISO": "PILAR", "FATIMA ESTACION EMPALME": "PILAR", "MANUEL ALBERTI": "PILAR",
    "PILAR": "PILAR", "PRESIDENTE DERQUI": "PILAR", "MANZANARES": "PILAR",
    "BELEN DE ESCOBAR": "ESCOBAR", "GARIN": "ESCOBAR", "INGENIERO MASCHWITZ": "ESCOBAR",
    "LOMA VERDE": "ESCOBAR", "MAQUINISTA SAVIO": "ESCOBAR", "MATHEU": "ESCOBAR", "VILLA ROSA": "ESCOBAR",
    "BENAVIDEZ": "TIGRE", "DIQUE LUJAN": "TIGRE", "DON TORCUATO": "TIGRE", "EL TALAR": "TIGRE",
    "GENERAL PACHECO": "TIGRE", "NORDELTA": "TIGRE", "SAN FERNANDO": "TIGRE", "TIGRE": "TIGRE",
    "VICTORIA": "TIGRE", "VIRREYES": "TIGRE", "RICARDO ROJAS": "TIGRE",
    "RINCON DE MILBERG": "TIGRE", "TRONCOS DEL TALAR": "TIGRE",
    "BECCAR": "ZN 1", "FLORIDA": "ZN 1", "MARTINEZ": "ZN 1", "OLIVOS": "ZN 1",
    "SAN ISIDRO": "ZN 1", "VICENTE LOPEZ": "ZN 1",
    "BOULOGNE": "ZN 2", "GENERAL SAN MARTIN": "ZN 2", "MUNRO": "ZN 2",
    "VILLA ADELINA": "ZN 2", "VILLA BALLESTER": "ZN 2",
    "CASTELAR": "OESTE", "FRANCISCO ALVAREZ": "OESTE", "GENERAL RODRIGUEZ": "OESTE",
    "ITUZAINGO": "OESTE", "MORENO": "OESTE", "MORON": "OESTE", "VILLA ASTOLFI": "OESTE",
    "CAPILLA DEL SEÑOR": "CAMPO", "LOS CARDALES": "CAMPO", "SAN ANTONIO DE ARECO": "CAMPO",
    "CAMPANA": "CAMPO", "EXALTACION DE LA CRUZ": "CAMPO", "PARADA ROBLES": "CAMPO",
    "SAN ANDRÉS": "A DEFINIR", "SAN JOSE": "A DEFINIR",
}


# ---------------------------------------------------------------------------
# 1) Conexión al API
# ---------------------------------------------------------------------------

def login(base_url, usuario, password):
    resp = requests.post(
        f"{base_url}/auth/login",
        json={"usuario": usuario, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    session_id = resp.json()["sessionId"]
    return {"Accept": "application/json", "Cookie": session_id}


def traer_ventas(base_url, headers, fecha_desde, fecha_hasta, max_lotes=100):
    todos = []
    params_base = {"fechadesde": fecha_desde, "fechahasta": fecha_hasta, "detallado": "true"}

    for lote in range(1, max_lotes + 1):
        params = params_base.copy()
        params["nroLote"] = lote
        resp = requests.get(f"{base_url}/ventas/", headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        ventas = resp.json().get("dsReporteComprobantesApi", {}).get("VentasResumen", [])
        if not ventas:
            break
        todos.extend(ventas)

    return pd.DataFrame(todos)


def meses_a_traer(hoy=None):
    """Devuelve las ventanas [(desde, hasta), ...] que hay que consultar:
       1) Mes ANTERIOR completo  (día 1 al último día de ese mes)
       2) Mes ACTUAL hasta hoy   (día 1 al día de hoy)
    Cada mes es una ventana separada: así nunca se mezcla un mes con otro.
    Funciona también en enero (el mes anterior cae en diciembre del año previo).
    """
    hoy = hoy or dt.date.today()
    primer_dia_actual = hoy.replace(day=1)
    ultimo_dia_anterior = primer_dia_actual - dt.timedelta(days=1)  # último día mes anterior
    primer_dia_anterior = ultimo_dia_anterior.replace(day=1)        # día 1 mes anterior
    return [
        (primer_dia_anterior, ultimo_dia_anterior),  # mes anterior completo
        (primer_dia_actual, hoy),                    # mes actual hasta hoy
    ]


def traer_ventas_meses(base_url, headers, ventanas):
    """Trae las ventas MES POR MES (la API no acepta rangos largos) y concatena.
    `ventanas` es una lista de (date_desde, date_hasta). Reutiliza traer_ventas,
    que internamente recorre los lotes (nroLote) hasta traerlos todos."""
    partes = []
    for desde, hasta in ventanas:
        fd = desde.strftime("%Y-%m-%d")
        fh = hasta.strftime("%Y-%m-%d")
        df_mes = traer_ventas(base_url, headers, fd, fh)
        print(f"  {fd} -> {fh}: {len(df_mes)} filas")
        if not df_mes.empty:
            partes.append(df_mes)
    return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()


# ---------------------------------------------------------------------------
# 2) Preparación / limpieza
# ---------------------------------------------------------------------------

def preparar(df_ventas):
    cols = [c for c in COLUMNAS_IMPORTANTES if c in df_ventas.columns]
    df_ventas = df_ventas[cols].copy()

    for c in COLUMNAS_NUMERICAS:
        if c in df_ventas.columns:
            df_ventas[c] = pd.to_numeric(df_ventas[c], errors="coerce").fillna(0)

    # Fecha real (no string). Formato de origen: YYYY-MM-DD
    df_ventas["fechaComprobate"] = pd.to_datetime(df_ventas["fechaComprobate"], errors="coerce")

    df_ventas["region"] = df_ventas["dsLocalidad"].map(MAPA_REGION).fillna("A DEFINIR")

    df_ventas = df_ventas[
        (df_ventas["anulado"].astype(str).str.upper().str.strip() == "NO")
        & (df_ventas["dsCanalMkt"].astype(str).str.upper().str.strip() != "VIANDAS")
        & (df_ventas["dsVendedor"].astype(str).str.upper().str.strip() != "DIRECTA")
        & (df_ventas["dsSubcanalMKT"].astype(str).str.upper().str.strip() != "VIANDAS")
        & (~df_ventas["idCliente"].isin(CLIENTES_EXCLUIR))
    ].copy()

    df_ventas["kilos"] = np.where(
        df_ventas["pesoTotal"] == 0, df_ventas["unimedtotal"], df_ventas["pesoTotal"]
    )
    df_ventas["Categoria"] = np.where(df_ventas["pesoTotal"] > 0, "Pesable", "No Pesable")
    df_ventas["costo_unitario"] = np.where(
        df_ventas["Categoria"] == "No Pesable",
        df_ventas["preciocomprant"] * df_ventas["cantidadesTotal"],
        df_ventas["preciocomprant"] * df_ventas["kilos"],
    )

    return df_ventas


# ---------------------------------------------------------------------------
# 3) Métricas (usadas por la app para mostrar; son cálculos livianos)
# ---------------------------------------------------------------------------

def metricas_generales(df_ventas):
    subtotal_neto = df_ventas["subtotalNeto"].sum()
    costo_total = df_ventas["costo_unitario"].sum()
    total_kilos = df_ventas["kilos"].sum()

    contribucion_marginal = subtotal_neto - costo_total
    cm_pct = (contribucion_marginal / subtotal_neto * 100) if subtotal_neto else 0
    precio_medio_kg = (subtotal_neto / total_kilos) if total_kilos else 0

    return {
        "total_kilos": total_kilos,
        "subtotal_neto": subtotal_neto,
        "costo_total": costo_total,
        "contribucion_marginal": contribucion_marginal,
        "cm_pct": cm_pct,
        "precio_medio_kg": precio_medio_kg,
    }


def kilos_por_region(df_ventas):
    return (df_ventas.groupby("region")["kilos"].sum()
            .sort_values(ascending=False).reset_index())


def kilos_por_empresa(df_ventas):
    return (df_ventas.groupby("dsEmpresa")["kilos"].sum()
            .sort_values(ascending=False).reset_index())


def subtotal_por_comprobante(df_ventas):
    return (df_ventas.groupby("dsDocumento")["subtotalNeto"].sum()
            .sort_values(ascending=False).reset_index())


def food_service(df_ventas):
    fs = df_ventas[
        df_ventas["dsCanalMkt"].astype(str).str.upper().str.strip() == "FOOD SERVICE"
    ].copy()

    subtotal_neto = fs["subtotalNeto"].sum()
    costo_total = (fs["preciocomprant"] * fs["cantidadesTotal"]).sum()
    cm = subtotal_neto - costo_total
    cm_pct = (cm / subtotal_neto * 100) if subtotal_neto else 0

    metricas = {
        "total_kilos": fs["kilos"].sum(),
        "subtotal_neto": subtotal_neto,
        "costo_total": costo_total,
        "contribucion_marginal": cm,
        "cm_pct": cm_pct,
    }
    return fs, metricas


def rfm(df_ventas):
    base = df_ventas.copy()
    base["fechaComprobate"] = pd.to_datetime(base["fechaComprobate"], errors="coerce")
    base = base.dropna(subset=["idCliente", "fechaComprobate"])
    if base.empty:
        return base

    fecha_analisis = base["fechaComprobate"].max()
    r = base.groupby("idCliente").agg(
        nombreCliente=("nombreCliente", "first"),
        ultima_compra=("fechaComprobate", "max"),
        frecuencia=("dsDocumento", "count"),
        monetario=("subtotalNeto", "sum"),
    ).reset_index()
    r["recencia"] = (fecha_analisis - r["ultima_compra"]).dt.days
    return r


# ---------------------------------------------------------------------------
# 4) Credenciales + persistencia
# ---------------------------------------------------------------------------

def cargar_credenciales():
    # 1) Variables de entorno (ideal para cron)
    if os.getenv("CHESS_PASSWORD"):
        return {
            "base_url": os.getenv("CHESS_BASE_URL", BASE_URL_DEFAULT),
            "usuario": os.getenv("CHESS_USUARIO", USUARIO_DEFAULT),
            "password": os.getenv("CHESS_PASSWORD"),
        }
    # 2) .streamlit/secrets.toml (mismo archivo que usa la app)
    ruta = os.path.join(".streamlit", "secrets.toml")
    if os.path.exists(ruta):
        import tomllib
        with open(ruta, "rb") as f:
            return tomllib.load(f)["chess"]
    raise RuntimeError(
        "No encontré credenciales. Definí CHESS_BASE_URL / CHESS_USUARIO / "
        "CHESS_PASSWORD como variables de entorno, o creá .streamlit/secrets.toml."
    )


def guardar(df_ventas):
    """Guarda el parquet de forma atómica (tmp + replace) y un metadata.json
    con la fecha/hora de última actualización."""
    os.makedirs(DATA_DIR, exist_ok=True)

    # Parquet: escribir en .tmp y luego reemplazar (lectura siempre consistente)
    tmp_parquet = PARQUET_PATH + ".tmp"
    df_ventas.to_parquet(tmp_parquet, index=False)
    os.replace(tmp_parquet, PARQUET_PATH)

    # Metadata: misma técnica atómica
    meta = {
        "ultima_actualizacion": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "filas": int(len(df_ventas)),
    }
    tmp_meta = META_PATH + ".tmp"
    with open(tmp_meta, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    os.replace(tmp_meta, META_PATH)


def main():
    cfg = cargar_credenciales()
    headers = login(cfg["base_url"], cfg["usuario"], cfg["password"])

    ventanas = meses_a_traer()
    desde = ventanas[0][0]   # día 1 del mes anterior
    hasta = ventanas[-1][1]  # hoy

    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] Trayendo ventas mes por mes "
          f"({desde:%Y-%m-%d} -> {hasta:%Y-%m-%d}) ...")
    df_raw = traer_ventas_meses(cfg["base_url"], headers, ventanas)

    if df_raw.empty:
        print("El API no devolvió filas. No se sobrescribe el archivo existente.")
        return

    df = preparar(df_raw)
    guardar(df)
    print(f"OK: {len(df)} filas guardadas en {PARQUET_PATH}")


if __name__ == "__main__":
    main()
