"""
data_pipeline.py
----------------
ETL: se conecta al API de Chess y mantiene data/ventas_actualizadas.parquet
con el DETALLE de ventas de TODO el año (ANIO), por UPSERT mensual:

  - En cada corrida SIEMPRE re-trae el MES ANTERIOR (completo) y el MES
    ACTUAL (del día 1 hasta hoy), porque pueden entrar comprobantes nuevos.
  - Además detecta qué meses del año FALTAN en el parquet y los trae UNA
    sola vez (auto-backfill). La primera corrida tarda más (baja todo el
    año); las siguientes vuelven a ser rápidas (solo actual + anterior).
  - Cada mes se guarda apenas se termina de traer (upsert atómico): si la
    conexión se corta, lo ya bajado queda persistido y la próxima corrida
    solo busca lo que falta.

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
import re
import json
import time
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

# Serie mensual histórica (agregada, liviana). Se construye UNA sola vez con
# backfill_serie.py y luego el cron solo corrige los meses que vuelve a traer.
SERIE_PATH = os.path.join(DATA_DIR, "serie_mensual.parquet")

# Fecha desde la que arranca la serie histórica (la usa el backfill).
SERIE_DESDE = dt.date(2025, 1, 1)

# IPC Nivel General Nacional (INDEC). Se usa para expresar la facturación en
# pesos CONSTANTES (ajustados por inflación) y poder comparar meses "con la
# misma vara". Se cachea local para no depender de que INDEC esté online.
IPC_URL = "https://www.indec.gob.ar/ftp/cuadros/economia/serie_ipc_divisiones.csv"
IPC_PATH = os.path.join(DATA_DIR, "ipc_indec.parquet")

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
# 0bis) Marca / Línea  (lookup por artículo)
# ---------------------------------------------------------------------------
# El API todavía no expone un campo confiable de "marca / línea". Agrupar por
# PROVEEDOR no alcanza: un mismo proveedor tiene varias líneas (RETAIL, FOOD
# SERVICE, REFRIGERADOS, ...) y además la marca comercial no se deduce del
# nombre del proveedor (ej. GARCIA HNOS -> TREGAR, ELCOR -> TONADITA,
# ERNESTO RODRIGUEZ -> VACALIN, FRIGORIFICO PALADINI -> PALADINI/FELA).
#
# Solución temporal: una TABLA FIJA artículo -> marca/línea extraída del tablero
# comercial de referencia (data/marca_linea_lookup.csv), que se joina por el
# nombre del artículo (dsArticulo). Los SKUs que no estén en la tabla caen al
# nombre del proveedor.
#
# Para actualizar la clasificación: editar/agregar filas en el CSV con las
# columnas  dsArticulo,marca_linea  (usando el nombre EXACTO del artículo).
LOOKUP_MARCA_PATH = os.path.join(DATA_DIR, "marca_linea_lookup.csv")

# DIAGNÓSTICO: con True, los artículos que NO están en la tabla se marcan
# "SIN REGLA · <proveedor>" en vez de caer al proveedor. Sirve para detectar
# SKUs sin clasificar. En producción dejar en False.
MARCA_LINEA_DEBUG = False


def _norm_articulo(s):
    """Normaliza el nombre de artículo para el join: mayúsculas y espacios
    colapsados (robusto a dobles espacios y a mayúsculas/minúsculas)."""
    return re.sub(r"\s+", " ", str(s).strip().upper())


def _prov_limpio(prov):
    """Saca el prefijo de código del proveedor: '9 - MC CAIN ...' -> 'MC CAIN ...'."""
    if prov is None:
        return ""
    return re.sub(r"^\s*\d+\s*-\s*", "", str(prov)).strip()


_LOOKUP_MARCA = None


def _cargar_lookup_marca(path=LOOKUP_MARCA_PATH):
    """Carga (y cachea en memoria) la tabla artículo -> marca/línea como dict
    con las claves normalizadas. Si el CSV no está, devuelve dict vacío."""
    global _LOOKUP_MARCA
    if _LOOKUP_MARCA is not None:
        return _LOOKUP_MARCA
    d = {}
    if os.path.exists(path):
        tab = pd.read_csv(path)
        for art, marca in zip(tab["dsArticulo"], tab["marca_linea"]):
            k = _norm_articulo(art)
            if k:
                d[k] = str(marca).strip()
    _LOOKUP_MARCA = d
    return d


def agregar_marca_linea(df):
    """Agrega/renueva la columna 'marca_linea' por lookup de dsArticulo contra
    data/marca_linea_lookup.csv. Fallback (artículo no listado) = nombre del
    proveedor sin el prefijo de código (o 'SIN REGLA · ...' si MARCA_LINEA_DEBUG).
    """
    if df is None:
        return df
    df = df.copy()
    if df.empty:
        df["marca_linea"] = pd.Series(dtype="object")
        return df

    lookup = _cargar_lookup_marca()

    if "dsArticulo" in df.columns:
        marca = df["dsArticulo"].map(_norm_articulo).map(lookup)
    else:
        marca = pd.Series([np.nan] * len(df), index=df.index)

    if "proveedor" in df.columns:
        prov_limpio = df["proveedor"].map(_prov_limpio)
    else:
        prov_limpio = pd.Series([""] * len(df), index=df.index)

    fallback = ("SIN REGLA · " + prov_limpio) if MARCA_LINEA_DEBUG else prov_limpio
    df["marca_linea"] = marca.where(marca.notna(), fallback)
    return df


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


def _ventana_mes(primer_dia, hoy):
    """(primer_dia, ultimo_dia) del mes calendario de `primer_dia`, cortado
    en `hoy` si el mes todavía no terminó."""
    if primer_dia.month == 12:
        fin = dt.date(primer_dia.year, 12, 31)
    else:
        fin = dt.date(primer_dia.year, primer_dia.month + 1, 1) - dt.timedelta(days=1)
    return primer_dia, min(fin, hoy)


def meses_detalle_esperados(hoy=None):
    """Primeros días de TODOS los meses que deberían estar en el parquet de
    detalle: de enero de ANIO hasta el mes actual (inclusive)."""
    hoy = hoy or dt.date.today()
    meses, cursor = [], dt.date(ANIO, 1, 1)
    while cursor <= hoy:
        meses.append(cursor)
        cursor = (cursor.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return meses


def meses_en_detalle(parquet_path=PARQUET_PATH):
    """Set de 'YYYY-MM' que ya están guardados en el parquet de detalle."""
    if not os.path.exists(parquet_path):
        return set()
    try:
        f = pd.read_parquet(parquet_path, columns=["fechaComprobate"])
        f = pd.to_datetime(f["fechaComprobate"], errors="coerce").dropna()
        return set(f.dt.strftime("%Y-%m").unique())
    except Exception:
        return set()


def ventanas_a_traer(hoy=None, parquet_path=PARQUET_PATH):
    """Ventanas [(desde, hasta), ...] que el pipeline debe pedir al API:

      1) SIEMPRE: mes anterior completo + mes actual hasta hoy (pueden
         haber entrado comprobantes nuevos o correcciones).
      2) ADEMÁS: los meses de ANIO que FALTEN en el parquet de detalle
         (auto-backfill). Solo pasa en la primera corrida o si un mes quedó
         a medias; después esta lista queda vacía y la corrida es rápida.

    Devuelve las ventanas ordenadas cronológicamente, una por mes.
    """
    hoy = hoy or dt.date.today()
    ventanas = {d.strftime("%Y-%m"): (d, h) for d, h in meses_a_traer(hoy)}
    ya = meses_en_detalle(parquet_path)
    for primer_dia in meses_detalle_esperados(hoy):
        mes = primer_dia.strftime("%Y-%m")
        if mes in ventanas or mes in ya:
            continue
        ventanas[mes] = _ventana_mes(primer_dia, hoy)
    return [ventanas[m] for m in sorted(ventanas)]


def traer_mes_seguro(cfg, headers, fecha_desde, fecha_hasta, max_reintentos=3):
    """Trae un mes reintentando (con re-login) si el servidor corta la
    conexión. Devuelve (df_mes, headers); headers puede renovarse."""
    for intento in range(1, max_reintentos + 1):
        try:
            return traer_ventas(cfg["base_url"], headers,
                                fecha_desde, fecha_hasta), headers
        except requests.exceptions.RequestException as e:
            if intento == max_reintentos:
                raise
            espera = 5 * intento
            print(f"    intento {intento}/{max_reintentos} falló "
                  f"({type(e).__name__}). Reintento en {espera}s...")
            time.sleep(espera)
            try:
                headers = login(cfg["base_url"], cfg["usuario"], cfg["password"])
            except Exception as e2:
                print(f"       (re-login falló: {type(e2).__name__}; "
                      f"se reintenta igual)")
    raise RuntimeError(f"No se pudo traer {fecha_desde} -> {fecha_hasta}")


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

    # Marca / línea por lookup de artículo (ver agregar_marca_linea).
    df_ventas = agregar_marca_linea(df_ventas)

    return df_ventas


# ---------------------------------------------------------------------------
# 3) Métricas (usadas por la app para mostrar; son cálculos livianos)
# ---------------------------------------------------------------------------

def comprobante_id(df_ventas):
    """Identificador único de comprobante (empresa + tipo doc + nº doc).
    Sirve para contar comprobantes y calcular el ticket promedio."""
    return (
        df_ventas["dsEmpresa"].astype(str) + "|"
        + df_ventas["dsDocumento"].astype(str) + "|"
        + df_ventas["nrodoc"].astype(str)
    )


def metricas_generales(df_ventas):
    subtotal_neto = df_ventas["subtotalNeto"].sum()
    costo_total = df_ventas["costo_unitario"].sum()
    total_kilos = df_ventas["kilos"].sum()

    contribucion_marginal = subtotal_neto - costo_total
    cm_pct = (contribucion_marginal / subtotal_neto * 100) if subtotal_neto else 0
    precio_medio_kg = (subtotal_neto / total_kilos) if total_kilos else 0

    n_clientes = df_ventas["idCliente"].nunique()
    n_comprobantes = comprobante_id(df_ventas).nunique()
    n_skus = df_ventas["idArticulo"].nunique() if "idArticulo" in df_ventas else 0
    ticket_promedio = (subtotal_neto / n_comprobantes) if n_comprobantes else 0
    kg_por_cliente = (total_kilos / n_clientes) if n_clientes else 0

    return {
        "total_kilos": total_kilos,
        "subtotal_neto": subtotal_neto,
        "costo_total": costo_total,
        "contribucion_marginal": contribucion_marginal,
        "cm_pct": cm_pct,
        "precio_medio_kg": precio_medio_kg,
        "n_clientes": n_clientes,
        "n_comprobantes": n_comprobantes,
        "n_skus": n_skus,
        "ticket_promedio": ticket_promedio,
        "kg_por_cliente": kg_por_cliente,
    }


def agrupar_dim(df_ventas, col):
    """Resumen por una dimensión cualquiera (canal, subcanal, vendedor,
    proveedor, artículo, etc.): kilos, facturación, costo, contribución,
    CM %, precio/kg, nº de clientes y share % sobre la facturación total."""
    g = (
        df_ventas.groupby(col)
        .agg(
            kilos=("kilos", "sum"),
            subtotalNeto=("subtotalNeto", "sum"),
            costo=("costo_unitario", "sum"),
            clientes=("idCliente", "nunique"),
            skus=("idArticulo", "nunique"),
        )
        .reset_index()
    )
    g["cm"] = g["subtotalNeto"] - g["costo"]
    g["cm_pct"] = np.where(g["subtotalNeto"] != 0, g["cm"] / g["subtotalNeto"] * 100, 0)
    g["precio_kg"] = np.where(g["kilos"] != 0, g["subtotalNeto"] / g["kilos"], 0)
    # Promedio real de SKUs distintos que compra cada cliente dentro de esta
    # dimensión: primero se cuentan los SKUs únicos por (dimensión, cliente)
    # y luego se promedia entre los clientes de cada grupo. (Antes se hacía
    # SKUs totales del grupo / clientes del grupo, que subestima el valor
    # cuando los clientes comparten productos entre sí.)
    skus_cliente = (
        df_ventas.groupby([col, "idCliente"])["idArticulo"]
        .nunique()
        .reset_index(name="_skus_cliente")
        .groupby(col)["_skus_cliente"]
        .mean()
    )
    g["skus_por_cliente"] = g[col].map(skus_cliente).fillna(0)
    total_fc = g["subtotalNeto"].sum()
    total_kg = g["kilos"].sum()
    g["share_fc"] = np.where(total_fc != 0, g["subtotalNeto"] / total_fc * 100, 0)
    g["share_kg"] = np.where(total_kg != 0, g["kilos"] / total_kg * 100, 0)
    return g.sort_values("subtotalNeto", ascending=False).reset_index(drop=True)


def por_canal(df_ventas):
    return agrupar_dim(df_ventas, "dsCanalMkt")


def por_subcanal(df_ventas):
    return agrupar_dim(df_ventas, "dsSubcanalMKT")


def por_vendedor(df_ventas):
    return agrupar_dim(df_ventas, "dsVendedor")


def por_proveedor(df_ventas):
    """'Marca / Línea': agrupa por la clasificación de negocio (marca_linea),
    NO por el proveedor crudo. La columna se arma con agregar_marca_linea()
    (lookup por artículo); si el df todavía no la tiene, se calcula al vuelo."""
    d = df_ventas if "marca_linea" in df_ventas.columns else agregar_marca_linea(df_ventas)
    return agrupar_dim(d, "marca_linea")


# --- Línea de producto "estricta" (para la solapa Líneas) ------------------
# A diferencia de marca_linea (que cae al proveedor cuando el artículo no está
# en el lookup), acá los SKUs sin regla caen a SIN_ASIGNAR. Así la solapa de
# gestión comercial los agrupa y los deja detectar sin romper nada.
SIN_ASIGNAR = "SIN ASIGNAR"


def agregar_linea_estricta(df, col_destino="linea_producto"):
    """Agrega la columna `linea_producto`: marca/línea del lookup por artículo,
    o SIN_ASIGNAR si el artículo no figura en data/marca_linea_lookup.csv.
    NO modifica 'marca_linea' ni afecta a las otras solapas."""
    if df is None:
        return df
    df = df.copy()
    if df.empty:
        df[col_destino] = pd.Series(dtype="object")
        return df
    lookup = _cargar_lookup_marca()
    if "dsArticulo" in df.columns:
        m = df["dsArticulo"].map(_norm_articulo).map(lookup)
    else:
        m = pd.Series([np.nan] * len(df), index=df.index)
    m = m.astype("object")
    vacia = m.isna() | (m.astype(str).str.strip() == "")
    df[col_destino] = m.where(~vacia, SIN_ASIGNAR)
    return df


def agrupar_multi(df_ventas, cols):
    """Como agrupar_dim pero por VARIAS dimensiones anidadas (ej. canal ×
    vendedor). Devuelve sumas crudas + cm, cm_pct, precio_kg, clientes, skus
    y share_fc / share_kg calculados sobre el total del df recibido (si el df
    ya viene filtrado a una línea, el share es "dentro de la línea")."""
    g = (
        df_ventas.groupby(list(cols), dropna=False)
        .agg(
            kilos=("kilos", "sum"),
            subtotalNeto=("subtotalNeto", "sum"),
            costo=("costo_unitario", "sum"),
            clientes=("idCliente", "nunique"),
            skus=("idArticulo", "nunique"),
        )
        .reset_index()
    )
    g["cm"] = g["subtotalNeto"] - g["costo"]
    g["cm_pct"] = np.where(g["subtotalNeto"] != 0, g["cm"] / g["subtotalNeto"] * 100, 0)
    g["precio_kg"] = np.where(g["kilos"] != 0, g["subtotalNeto"] / g["kilos"], 0)
    total_fc = g["subtotalNeto"].sum()
    total_kg = g["kilos"].sum()
    g["share_fc"] = np.where(total_fc != 0, g["subtotalNeto"] / total_fc * 100, 0)
    g["share_kg"] = np.where(total_kg != 0, g["kilos"] / total_kg * 100, 0)
    return g.sort_values("subtotalNeto", ascending=False).reset_index(drop=True)


def ranking_productos(df_ventas):
    """Ranking de SKUs con clasificación ABC (Pareto sobre facturación):
    A = hasta el 80 % acumulado, B = 80-95 %, C = el resto."""
    g = agrupar_dim(df_ventas, "dsArticulo")
    total = g["subtotalNeto"].sum()
    g["pct_acum"] = (g["subtotalNeto"].cumsum() / total * 100) if total else 0

    def clase(p):
        if p <= 80:
            return "A"
        if p <= 95:
            return "B"
        return "C"

    g["ABC"] = g["pct_acum"].apply(clase)
    return g


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

    # El df tiene una fila por línea de artículo; para contar compras reales
    # usamos el comprobante único (empresa + tipo doc + nº doc), no las filas.
    base["_comp_id"] = comprobante_id(base)

    fecha_analisis = base["fechaComprobate"].max()
    r = base.groupby("idCliente").agg(
        nombreCliente=("nombreCliente", "first"),
        ultima_compra=("fechaComprobate", "max"),
        frecuencia=("_comp_id", "nunique"),
        monetario=("subtotalNeto", "sum"),
    ).reset_index()
    r["recencia"] = (fecha_analisis - r["ultima_compra"]).dt.days

    # --- Scores 1-4 y segmentación RFM ---------------------------------
    def _score(serie, invertir=False):
        # rank(method="first") evita errores de bins duplicados en qcut
        try:
            etiquetas = [4, 3, 2, 1] if invertir else [1, 2, 3, 4]
            return pd.qcut(serie.rank(method="first"), 4, labels=etiquetas).astype(int)
        except (ValueError, IndexError):
            return pd.Series(1, index=serie.index)

    r["r_score"] = _score(r["recencia"], invertir=True)   # menos recencia = mejor
    r["f_score"] = _score(r["frecuencia"])
    r["m_score"] = _score(r["monetario"])

    def _segmento(row):
        if row.r_score >= 3 and row.f_score >= 3 and row.m_score >= 3:
            return "Campeones"
        if row.r_score >= 3 and row.f_score >= 2:
            return "Leales"
        if row.r_score >= 3:
            return "Nuevos / Prometedores"
        if row.f_score >= 3 or row.m_score >= 3:
            return "En riesgo"
        return "Hibernando / Perdidos"

    r["segmento"] = r.apply(_segmento, axis=1)
    return r


def resumen_segmentos(df_rfm):
    """Cuenta de clientes y facturación por segmento RFM."""
    if df_rfm.empty:
        return df_rfm
    g = (
        df_rfm.groupby("segmento")
        .agg(clientes=("idCliente", "count"), facturacion=("monetario", "sum"))
        .reset_index()
        .sort_values("facturacion", ascending=False)
    )
    return g


def altas_bajas(df_ventas, hoy=None):
    """Altas y bajas de clientes entre un mes de REFERENCIA y su anterior.

    `hoy` es la fecha de referencia (default: hoy). El "mes actual" es el mes
    de esa fecha, cortado en esa fecha; el "anterior" es el mes previo
    completo. Pasando hoy=último día de un mes cerrado, compara ese mes
    completo contra su anterior (así la app lo usa para cualquier mes de 2026).

    - Altas: compraron este mes y NO el mes pasado.
    - Bajas: compraron el mes pasado y NO este mes.

    Recibe el df SIN filtrar por período (necesita ver ambos meses).
    Devuelve (altas, bajas): un df por lado con compras, kilos, facturación
    y fecha de última compra por cliente.
    """
    hoy = hoy or dt.date.today()
    base = df_ventas.copy()
    base["fechaComprobate"] = pd.to_datetime(base["fechaComprobate"], errors="coerce")
    base = base.dropna(subset=["idCliente", "fechaComprobate"])

    ini_act = pd.Timestamp(hoy.replace(day=1))            # 1° del mes actual
    ini_ant = pd.Timestamp((hoy.replace(day=1) - dt.timedelta(days=1)).replace(day=1))
    fin_act = pd.Timestamp(hoy) + pd.Timedelta(days=1)    # hasta hoy inclusive

    f = base["fechaComprobate"]
    m_act = base[(f >= ini_act) & (f < fin_act)]
    m_ant = base[(f >= ini_ant) & (f < ini_act)]

    def _resumen(d):
        if d.empty:
            return pd.DataFrame(columns=[
                "idCliente", "nombreCliente", "compras",
                "kilos", "facturacion", "ultima_compra",
            ])
        d = d.copy()
        d["_comp_id"] = comprobante_id(d)
        return d.groupby("idCliente").agg(
            nombreCliente=("nombreCliente", "first"),
            compras=("_comp_id", "nunique"),
            kilos=("kilos", "sum"),
            facturacion=("subtotalNeto", "sum"),
            ultima_compra=("fechaComprobate", "max"),
        ).reset_index()

    res_act = _resumen(m_act)
    res_ant = _resumen(m_ant)

    altas = res_act[~res_act["idCliente"].isin(set(res_ant["idCliente"]))]
    bajas = res_ant[~res_ant["idCliente"].isin(set(res_act["idCliente"]))]

    altas = altas.sort_values("facturacion", ascending=False).reset_index(drop=True)
    bajas = bajas.sort_values("facturacion", ascending=False).reset_index(drop=True)
    return altas, bajas


def alertas(df_ventas):
    """Alertas e insights automáticos (lista de dicts: nivel + texto)."""
    avisos = []

    # 1) Productos con margen bruto negativo
    prod = ranking_productos(df_ventas)
    neg = prod[prod["cm"] < 0].sort_values("cm")
    if len(neg):
        tops = ", ".join(neg["dsArticulo"].head(3).astype(str))
        avisos.append({
            "nivel": "riesgo",
            "texto": f"{len(neg)} producto(s) con margen bruto NEGATIVO. "
                     f"Mayor pérdida: {tops}.",
        })

    # 2) Concentración de facturación en el top 10 de clientes
    r = rfm(df_ventas)
    if not r.empty:
        total = r["monetario"].sum()
        top10 = r.sort_values("monetario", ascending=False).head(10)["monetario"].sum()
        pct = (top10 / total * 100) if total else 0
        nivel = "riesgo" if pct >= 50 else "info"
        avisos.append({
            "nivel": nivel,
            "texto": f"El top 10 de clientes concentra el {pct:.0f}% de la facturación.",
        })

    # 3) Canal de menor margen
    can = por_canal(df_ventas)
    if not can.empty:
        peor = can.sort_values("cm_pct").iloc[0]
        avisos.append({
            "nivel": "info" if peor["cm_pct"] >= 0 else "riesgo",
            "texto": f"Canal de menor margen: {peor['dsCanalMkt']} "
                     f"(CM {peor['cm_pct']:.1f}%).",
        })

    # 4) Concentración de SKUs (Pareto)
    n_a = int((prod["ABC"] == "A").sum())
    n_tot = len(prod)
    if n_tot:
        avisos.append({
            "nivel": "info",
            "texto": f"{n_a} de {n_tot} SKUs (clase A) generan el 80% de la facturación.",
        })

    return avisos


# ---------------------------------------------------------------------------
# 3bis) Serie mensual agregada (para la solapa de Evolución)
# ---------------------------------------------------------------------------

# Grano de la serie histórica. Guardamos a este nivel; en la app se puede
# "subir" a canal, subcanal o vendedor sumando el resto de las dimensiones
# (las sumas se re-agregan sin problema porque son crudas, no porcentajes).
SERIE_GRANO = ["anio_mes", "dsCanalMkt", "dsSubcanalMKT", "dsVendedor"]
SERIE_COLS = SERIE_GRANO + [
    "kilos", "subtotalNeto", "costo", "cm", "clientes", "comprobantes"
]


def agregar_serie(df_ventas):
    """Agrega el detalle a nivel mes × canal × subcanal × vendedor, guardando
    SOLO sumas crudas. NUNCA guardamos porcentajes (CM %, share, $/kg): esos
    se derivan al leer, porque un promedio de porcentajes no se puede
    re-agregar bien.

    Columnas de salida (SERIE_COLS):
      anio_mes (YYYY-MM), dsCanalMkt, dsSubcanalMKT, dsVendedor,
      kilos, subtotalNeto, costo, cm, clientes, comprobantes

    Nota: 'clientes' y 'comprobantes' son conteos únicos POR FILA (mes×canal×
    subcanal×vendedor). Sirven para graficar por mes, pero no se deben sumar
    entre meses ni entre subcanales/vendedores para sacar un único total (se
    duplicarían clientes que compran en varios subcanales o le compran a más
    de un vendedor).
    """
    if df_ventas is None or df_ventas.empty:
        return pd.DataFrame(columns=SERIE_COLS)

    d = df_ventas.copy()
    d["anio_mes"] = d["fechaComprobate"].dt.to_period("M").astype(str)
    d["_comp"] = comprobante_id(d)

    g = (
        d.groupby(SERIE_GRANO, dropna=False)
        .agg(
            kilos=("kilos", "sum"),
            subtotalNeto=("subtotalNeto", "sum"),
            costo=("costo_unitario", "sum"),
            clientes=("idCliente", "nunique"),
            comprobantes=("_comp", "nunique"),
        )
        .reset_index()
    )
    g["cm"] = g["subtotalNeto"] - g["costo"]
    return g[SERIE_COLS].sort_values(
        ["anio_mes"] + SERIE_GRANO[1:]
    ).reset_index(drop=True)


def upsert_serie(df_detalle, serie_path=SERIE_PATH):
    """Inserta/actualiza en la serie histórica los meses presentes en
    `df_detalle` (en el cron: mes actual + anterior).

    Mecanismo: borra de la serie las filas de ESOS meses y las reemplaza por las
    recién calculadas. Los meses que no aparecen en df_detalle quedan intactos
    (nunca se vuelven a pedir al API). Es idempotente: correrlo 1 o N veces da el
    mismo resultado. Escritura atómica (tmp + replace).
    """
    nuevos = agregar_serie(df_detalle)
    if nuevos.empty:
        print("  serie: el detalle no tiene filas; serie sin cambios.")
        return None

    meses_nuevos = set(nuevos["anio_mes"].unique())

    if os.path.exists(serie_path):
        actual = pd.read_parquet(serie_path)
        actual = actual[~actual["anio_mes"].isin(meses_nuevos)]
        serie = pd.concat([actual, nuevos], ignore_index=True)
    else:
        serie = nuevos

    serie = serie.sort_values(
        ["anio_mes"] + SERIE_GRANO[1:]
    ).reset_index(drop=True)

    os.makedirs(os.path.dirname(serie_path) or ".", exist_ok=True)
    tmp = serie_path + ".tmp"
    serie.to_parquet(tmp, index=False)
    os.replace(tmp, serie_path)
    print(f"  serie: meses actualizados {sorted(meses_nuevos)} · "
          f"{len(serie)} filas totales en {serie_path}")
    return serie


# ---------------------------------------------------------------------------
# 3ter) IPC INDEC + deflactor (pesos constantes)
# ---------------------------------------------------------------------------

def descargar_ipc(url=IPC_URL):
    """Baja la serie de IPC del INDEC y devuelve un DataFrame con columnas
    ['anio_mes' (YYYY-MM), 'ipc'] del Nivel General Nacional."""
    df = pd.read_csv(url, sep=";", encoding="latin1", na_values=["NA"])
    df = df[(df["Region"] == "Nacional")
            & (df["Descripcion"] == "NIVEL GENERAL")].copy()
    df["anio_mes"] = pd.to_datetime(
        df["Periodo"].astype(str), format="%Y%m"
    ).dt.strftime("%Y-%m")
    df["ipc"] = pd.to_numeric(
        df["Indice_IPC"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    return (df[["anio_mes", "ipc"]].dropna()
            .sort_values("anio_mes").reset_index(drop=True))


def actualizar_ipc(ipc_path=IPC_PATH):
    """Baja el IPC y lo guarda (atómico). Si INDEC no responde, deja el archivo
    anterior intacto y avisa. Devuelve el DataFrame guardado (o None)."""
    try:
        ipc = descargar_ipc()
    except Exception as e:  # red caída, formato cambiado, etc.
        print(f"  IPC: no se pudo actualizar ({type(e).__name__}: {e}). "
              f"Se mantiene el archivo guardado si existe.")
        return None
    if ipc.empty:
        print("  IPC: descarga vacía; no se sobrescribe.")
        return None
    os.makedirs(os.path.dirname(ipc_path) or ".", exist_ok=True)
    tmp = ipc_path + ".tmp"
    ipc.to_parquet(tmp, index=False)
    os.replace(tmp, ipc_path)
    print(f"  IPC: {len(ipc)} meses guardados ({ipc['anio_mes'].iloc[0]} → "
          f"{ipc['anio_mes'].iloc[-1]}) en {ipc_path}")
    return ipc


def cargar_ipc(ipc_path=IPC_PATH):
    """Lee el IPC cacheado. Devuelve DataFrame vacío si todavía no existe."""
    if os.path.exists(ipc_path):
        return pd.read_parquet(ipc_path)
    return pd.DataFrame(columns=["anio_mes", "ipc"])


def factores_constantes(ipc_df, base_mes=None):
    """Devuelve (factores, base_mes) para llevar pesos corrientes a pesos
    CONSTANTES del mes base: factor[mes] = ipc_base / ipc[mes].

    base_mes por defecto = el último mes con IPC publicado (así todo queda en
    "pesos de hoy"). Multiplicar una facturación corriente por su factor la
    expresa en pesos del mes base.
    """
    if ipc_df is None or ipc_df.empty:
        return {}, None
    ipc = ipc_df.dropna(subset=["ipc"]).sort_values("anio_mes")
    meses = set(ipc["anio_mes"])
    if base_mes is None or base_mes not in meses:
        base_mes = ipc["anio_mes"].iloc[-1]
    ipc_base = float(ipc.loc[ipc["anio_mes"] == base_mes, "ipc"].iloc[0])
    factores = {r.anio_mes: ipc_base / float(r.ipc) for r in ipc.itertuples()}
    return factores, base_mes


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


def guardar(df_ventas, parquet_path=PARQUET_PATH):
    """UPSERT del parquet de DETALLE (mismo mecanismo que upsert_serie).

    Borra del parquet los meses presentes en `df_ventas` y los reemplaza por
    las filas recién traídas. Los meses que NO vienen en df_ventas quedan
    intactos (nunca se vuelven a pedir al API). Idempotente y atómico
    (tmp + replace). También actualiza metadata.json.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    fechas = pd.to_datetime(df_ventas["fechaComprobate"], errors="coerce")
    meses_nuevos = set(fechas.dropna().dt.strftime("%Y-%m").unique())

    if os.path.exists(parquet_path) and meses_nuevos:
        actual = pd.read_parquet(parquet_path)
        f_act = pd.to_datetime(actual["fechaComprobate"], errors="coerce")
        actual = actual[~f_act.dt.strftime("%Y-%m").isin(meses_nuevos)]
        total = pd.concat([actual, df_ventas], ignore_index=True)
    else:
        total = df_ventas

    total = total.sort_values("fechaComprobate").reset_index(drop=True)

    # Parquet: escribir en .tmp y luego reemplazar (lectura siempre consistente)
    tmp_parquet = parquet_path + ".tmp"
    total.to_parquet(tmp_parquet, index=False)
    os.replace(tmp_parquet, parquet_path)

    meses_total = sorted(
        pd.to_datetime(total["fechaComprobate"], errors="coerce")
        .dropna().dt.strftime("%Y-%m").unique()
    )

    # Metadata: misma técnica atómica
    meta = {
        "ultima_actualizacion": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "filas": int(len(total)),
        "meses": meses_total,
    }
    tmp_meta = META_PATH + ".tmp"
    with open(tmp_meta, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    os.replace(tmp_meta, META_PATH)
    return total


def main():
    cfg = cargar_credenciales()
    headers = login(cfg["base_url"], cfg["usuario"], cfg["password"])

    # Mes actual + anterior SIEMPRE, más los meses de ANIO que falten en el
    # parquet (auto-backfill: solo la primera vez o si un mes quedó a medias).
    ventanas = ventanas_a_traer()
    meses = [d.strftime("%Y-%m") for d, _ in ventanas]
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] Meses a traer: "
          f"{', '.join(meses)}")
    if len(ventanas) > 2:
        print(f"  ({len(ventanas) - 2} mes(es) faltantes en el parquet: se "
              f"traen UNA sola vez; las próximas corridas vuelven a ser solo "
              f"mes actual + anterior)")

    procesados = 0
    for desde, hasta in ventanas:
        fd, fh = desde.strftime("%Y-%m-%d"), hasta.strftime("%Y-%m-%d")
        try:
            df_raw, headers = traer_mes_seguro(cfg, headers, fd, fh)
        except requests.exceptions.RequestException as e:
            print(f"  {fd} -> {fh}: ERROR ({type(e).__name__}: {e}). "
                  f"Ese mes queda pendiente para la próxima corrida.")
            continue

        print(f"  {fd} -> {fh}: {len(df_raw)} filas")
        if df_raw.empty:
            continue

        df_mes = preparar(df_raw)

        # Guarda YA este mes en el detalle (upsert atómico): si se corta el
        # siguiente, lo bajado queda persistido y la próxima corrida solo
        # busca lo que falta.
        guardar(df_mes)

        # Y actualiza la serie mensual histórica con el mismo mes.
        upsert_serie(df_mes)
        procesados += 1

    if procesados:
        print(f"OK: {procesados} mes(es) actualizados en {PARQUET_PATH}")
    else:
        print("El API no devolvió filas. Archivos existentes sin cambios.")

    # Refresca el IPC del INDEC (para los pesos constantes de la app).
    actualizar_ipc()


if __name__ == "__main__":
    main()
