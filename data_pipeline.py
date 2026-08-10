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
import warnings
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

# Acuerdos comerciales McCain: descuentos en el costo por cliente-artículo-mes
# que el reporte de Chess NO incluye. Se cargan por Excel desde el tablero
# (solapa "Acuerdos McCain") y se consolidan acá. El costo del parquet de
# ventas queda SIEMPRE crudo (tal como viene de Chess); el ajuste se aplica
# al leer, con aplicar_acuerdos().
ACUERDOS_PATH = os.path.join(DATA_DIR, "acuerdos_mccain.parquet")

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

# Réplica del filtro "Flt Art = SI" del Excel comercial (2026 Conversor):
# artículos que no son mercadería real y distorsionan la contribución.
#   0    -> líneas de CONCEPTOS: NC/ND por diferencia de precios, acuerdos
#           comerciales, rechazo de cheques. Sin costo asociado.
#   1000 -> VIANDAS DE REFRIGERIO (artículo dado de baja).
# Criterio acordado con Tomás (jul-2026) para que el tablero y el Excel
# comercial midan lo mismo.
ARTICULOS_EXCLUIR = [0, 1000]

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
# 0bis) Marca / Línea  (lookup por CÓDIGO de artículo)
# ---------------------------------------------------------------------------
# El API todavía no expone un campo confiable de "marca / línea". Agrupar por
# PROVEEDOR no alcanza: un mismo proveedor tiene varias líneas (RETAIL, FOOD
# SERVICE, REFRIGERADOS, ...) y además la marca comercial no se deduce del
# nombre del proveedor (ej. GARCIA HNOS -> TREGAR, ELCOR -> TONADITA,
# ERNESTO RODRIGUEZ -> VACALIN, FRIGORIFICO PALADINI -> PALADINI/FELA).
#
# Clasificación: una TABLA FIJA por CÓDIGO de artículo -> marca/línea
# (data/proveedor_objetivo_lookup.csv). El valor 'marca_linea' es el
# "PROVEEDOR OBJETIVO" del tablero comercial: se precalcula una sola vez por
# código replicando exactamente la fórmula del Excel (reglas por proveedor +
# grupo/familia/línea/código + tabla FOOD para FRIAR). Como grupo, familia,
# línea y proveedor son atributos fijos del artículo, el resultado es constante
# por código; por eso se joina por idArticulo (== "Código de Artículo" del ERP),
# que es exacto y no depende del texto del nombre.
#
# Los SKUs que no estén en la tabla (artículos nuevos aún no clasificados) caen
# al nombre del proveedor. Para actualizarla, regenerar el CSV desde el Excel
# comercial (ver build_lookup_proveedor_objetivo.py).
LOOKUP_MARCA_PATH = os.path.join(DATA_DIR, "proveedor_objetivo_lookup.csv")

# DIAGNÓSTICO: con True, los artículos que NO están en la tabla se marcan
# "SIN REGLA · <proveedor>" en vez de caer al proveedor. Sirve para detectar
# SKUs sin clasificar. En producción dejar en False.
MARCA_LINEA_DEBUG = False


def _prov_limpio(prov):
    """Saca el prefijo de código del proveedor: '9 - MC CAIN ...' -> 'MC CAIN ...'."""
    if prov is None:
        return ""
    return re.sub(r"^\s*\d+\s*-\s*", "", str(prov)).strip()


_LOOKUP_MARCA = None


def _cargar_lookup_marca(path=LOOKUP_MARCA_PATH):
    """Carga (y cachea en memoria) la tabla CÓDIGO -> marca/línea como dict
    {idArticulo(int): marca_linea(str)}. Si el CSV no está, devuelve dict vacío."""
    global _LOOKUP_MARCA
    if _LOOKUP_MARCA is not None:
        return _LOOKUP_MARCA
    d = {}
    if os.path.exists(path):
        tab = pd.read_csv(path)
        for cod, marca in zip(tab["idArticulo"], tab["marca_linea"]):
            if pd.isna(cod):
                continue
            m = str(marca).strip()
            if m and m.upper() != "NO":
                d[int(cod)] = m
    _LOOKUP_MARCA = d
    return d


def _marca_por_codigo(df):
    """Serie con la marca/línea de cada fila según idArticulo (NaN si el código
    no está en el lookup)."""
    lookup = _cargar_lookup_marca()
    if "idArticulo" in df.columns:
        cod = pd.to_numeric(df["idArticulo"], errors="coerce")
        return cod.map(lookup)
    return pd.Series([np.nan] * len(df), index=df.index, dtype="object")


def agregar_marca_linea(df):
    """Agrega/renueva la columna 'marca_linea' por lookup de idArticulo contra
    data/proveedor_objetivo_lookup.csv. Fallback (código no listado) = nombre del
    proveedor sin el prefijo de código (o 'SIN REGLA · ...' si MARCA_LINEA_DEBUG).
    """
    if df is None:
        return df
    df = df.copy()
    if df.empty:
        df["marca_linea"] = pd.Series(dtype="object")
        return df

    marca = _marca_por_codigo(df)

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
        & (~pd.to_numeric(df_ventas["idArticulo"], errors="coerce")
           .isin(ARTICULOS_EXCLUIR))
    ].copy()

    df_ventas = recalcular_costo(df_ventas)

    # Marca / línea por lookup de artículo (ver agregar_marca_linea).
    df_ventas = agregar_marca_linea(df_ventas)

    return df_ventas


def recalcular_costo(df):
    """Columnas derivadas de kilos y costo. MISMA LÓGICA que el Excel
    comercial (solapa Formulador del 2026 Conversor), para que el tablero y
    el reporte del Excel den idéntico. Criterios acordados con Tomás (jul-2026):

    kilos        -> todo lo que salió, incluido lo bonificado. pesoTotal, o
                    unimedtotal si el artículo no registra peso.
                    (= columna "Kg vendidos" del Excel)
    kilos_cargo  -> solo lo COBRADO. pesoTotal, o unimedcargo si no hay peso;
                    0 si la línea no tiene bultos. (= columna "Kilos" del Excel)
    bultos_cargo -> bultos cobrados. La API no lo informa directo; se deriva
                    prorrateando por unimedcargo/unimedtotal (en la práctica
                    las líneas son 100% cobradas o 100% bonificadas, no hay
                    mixtas, así que el prorrateo da todo o nada).
    Categoria    -> Pesable si pesoTotal != 0. OJO: != 0 y no > 0, porque las
                    devoluciones (NC) tienen peso NEGATIVO y son pesables; con
                    "> 0" se les revertía el costo por bulto en vez de por kilo.
    costo_unitario -> Pesable: preciocomprant x kilos_cargo
                      No pesable: preciocomprant x bultos_cargo
                    La mercadería bonificada (sin cargo) NO lleva costo: el
                    proveedor la repone (confirmado para McCain; mismo criterio
                    que el Excel comercial para el resto).

    Es idempotente y trabaja solo con columnas crudas de la API, así que
    sirve tanto en preparar() como para re-derivar un parquet ya guardado.
    """
    df = df.copy()
    df["kilos"] = np.where(
        df["pesoTotal"] == 0, df["unimedtotal"], df["pesoTotal"]
    )
    df["kilos_cargo"] = np.where(
        df["cantidadesTotal"] == 0, 0.0,
        np.where(df["pesoTotal"] == 0, df["unimedcargo"], df["pesoTotal"]),
    )
    # Prorrateo seguro: si unimedtotal es 0 (línea sin unidades de medida)
    # se asume todo con cargo, como hacía la fórmula anterior.
    _um_total = df["unimedtotal"].replace(0, np.nan)
    _frac_cargo = (df["unimedcargo"] / _um_total).fillna(1.0)
    df["bultos_cargo"] = df["cantidadesTotal"] * _frac_cargo
    df["Categoria"] = np.where(df["pesoTotal"] != 0, "Pesable", "No Pesable")
    df["costo_unitario"] = np.where(
        df["Categoria"] == "No Pesable",
        df["preciocomprant"] * df["bultos_cargo"],
        df["preciocomprant"] * df["kilos_cargo"],
    )
    return df


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
    total_cm = g["cm"].sum()
    g["share_fc"] = np.where(total_fc != 0, g["subtotalNeto"] / total_fc * 100, 0)
    g["share_kg"] = np.where(total_kg != 0, g["kilos"] / total_kg * 100, 0)
    g["share_cm"] = np.where(total_cm != 0, g["cm"] / total_cm * 100, 0)
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
# A diferencia de marca_linea (que cae al proveedor cuando el código no está
# en el lookup), acá los SKUs sin regla caen a SIN_ASIGNAR. Así la solapa de
# gestión comercial los agrupa y los deja detectar sin romper nada.
SIN_ASIGNAR = "SIN ASIGNAR"


def agregar_linea_estricta(df, col_destino="linea_producto"):
    """Agrega la columna `linea_producto`: marca/línea del lookup por código de
    artículo, o SIN_ASIGNAR si el código no figura en
    data/proveedor_objetivo_lookup.csv. NO modifica 'marca_linea' ni afecta a
    las otras solapas."""
    if df is None:
        return df
    df = df.copy()
    if df.empty:
        df[col_destino] = pd.Series(dtype="object")
        return df
    m = _marca_por_codigo(df).astype("object")
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
    total_cm = g["cm"].sum()
    g["share_fc"] = np.where(total_fc != 0, g["subtotalNeto"] / total_fc * 100, 0)
    g["share_kg"] = np.where(total_kg != 0, g["kilos"] / total_kg * 100, 0)
    g["share_cm"] = np.where(total_cm != 0, g["cm"] / total_cm * 100, 0)
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
    # costo_unitario ya trae la lógica oficial (pesable/no pesable, sin cargo
    # a costo 0, ajuste McCain si se aplicó antes). Antes acá se recalculaba
    # precio x cantidades, que daba distinto al resto del tablero.
    costo_total = fs["costo_unitario"].sum()
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
# 3bis-b) Acuerdos comerciales McCain (descuento en el costo por
#         cliente-artículo-mes que Chess no informa)
# ---------------------------------------------------------------------------
# Flujo mensual: McCain manda un Excel con los acuerdos vigentes (los valores
# cambian todos los meses por inflación). Desde el tablero se sube el archivo,
# se valida y se hace UPSERT POR MES en ACUERDOS_PATH: los meses que trae el
# Excel nuevo reemplazan a los guardados; los demás quedan intactos. Así da
# igual si el Excel trae solo el mes nuevo o el acumulado del año.

# Columnas del almacén consolidado. desc_kg es $/kg a RESTAR del costo.
ACUERDOS_COLS = ["anio", "mes", "idCliente", "idArticulo", "desc_kg"]

# Nombres esperados en el Excel (hoja "Descuentos"), normalizados a minúscula
# y sin espacios extra -> nombre interno.
_ACUERDOS_MAPA_COLS = {
    "año": "anio",
    "ano": "anio",
    "mes": "mes",
    "cod cliente": "idCliente",
    "cod artic": "idArticulo",
    "desc en el costo": "desc_kg",
}


def procesar_excel_acuerdos(archivo):
    """Lee y valida un Excel de acuerdos McCain. Devuelve (df, resumen).

    `archivo` puede ser una ruta o un file-like (st.file_uploader).
    Tolerante al formato: busca la hoja y la fila de encabezado que contengan
    'Desc en el costo', así el Excel puede traer títulos arriba o columnas
    en otro orden sin romper la carga.

    Reglas (definidas con Juan):
      - filas sin cliente/artículo/mes/valor -> se descartan
      - clave cliente-artículo-mes duplicada -> gana la ÚLTIMA fila del Excel
      - valores negativos -> se aplican con su signo (suben el costo), pero
        se informan en el resumen para poder auditarlos
    """
    xls = pd.ExcelFile(archivo)
    hoja_ok, header_idx = None, None
    for hoja in xls.sheet_names:
        crudo = xls.parse(hoja, header=None, nrows=15)
        for i, fila in crudo.iterrows():
            vals = [str(v).strip().lower() for v in fila.tolist()]
            if "desc en el costo" in vals:
                hoja_ok, header_idx = hoja, i
                break
        if hoja_ok is not None:
            break
    if hoja_ok is None:
        raise ValueError(
            "No encontré la columna 'Desc en el costo' en ninguna hoja. "
            "¿Es el Excel de acuerdos McCain con el formato de siempre?"
        )

    df = xls.parse(hoja_ok, header=header_idx)
    df.columns = [str(c).strip().lower() for c in df.columns]
    df = df.rename(columns=_ACUERDOS_MAPA_COLS)

    faltan = [c for c in ACUERDOS_COLS if c not in df.columns]
    if faltan:
        raise ValueError(f"Faltan columnas en el Excel: {faltan}")

    df = df[ACUERDOS_COLS].copy()
    filas_leidas = len(df)

    # Numéricos + descarte de filas incompletas (las vacías del final, etc.)
    for c in ACUERDOS_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna()
    for c in ["anio", "mes", "idCliente", "idArticulo"]:
        df[c] = df[c].astype(int)
    descartadas = filas_leidas - len(df)

    # Duplicados por clave: gana la última aparición en el archivo.
    clave = ["anio", "mes", "idCliente", "idArticulo"]
    duplicados = int(df.duplicated(subset=clave).sum())
    df = df.drop_duplicates(subset=clave, keep="last").reset_index(drop=True)

    resumen = {
        "filas_leidas": filas_leidas,
        "filas_validas": len(df),
        "descartadas": descartadas,
        "duplicados_resueltos": duplicados,
        "negativos": int((df["desc_kg"] < 0).sum()),
        "meses": sorted(
            f"{int(a)}-{int(m):02d}"
            for a, m in df[["anio", "mes"]].drop_duplicates().itertuples(index=False)
        ),
    }
    return df, resumen


def cargar_acuerdos(path=ACUERDOS_PATH):
    """Acuerdos consolidados. DataFrame vacío (con columnas) si aún no hay."""
    if not os.path.exists(path):
        return pd.DataFrame(columns=ACUERDOS_COLS)
    return pd.read_parquet(path)


def upsert_acuerdos(df_nuevos, path=ACUERDOS_PATH):
    """UPSERT POR MES (mismo mecanismo que upsert_serie): borra del almacén
    los (año, mes) presentes en `df_nuevos` y los reemplaza. Idempotente,
    escritura atómica."""
    actual = cargar_acuerdos(path)
    meses_nuevos = set(map(tuple, df_nuevos[["anio", "mes"]].drop_duplicates().values))
    if not actual.empty:
        clave_actual = list(map(tuple, actual[["anio", "mes"]].values))
        actual = actual[[k not in meses_nuevos for k in clave_actual]]
        total = pd.concat([actual, df_nuevos], ignore_index=True)
    else:
        total = df_nuevos.copy()
    total = total.sort_values(
        ["anio", "mes", "idCliente", "idArticulo"]
    ).reset_index(drop=True)

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    total.to_parquet(tmp, index=False)
    os.replace(tmp, path)
    return total


def aplicar_acuerdos(df_ventas, acuerdos=None):
    """Ajusta el costo de las ventas con los acuerdos McCain.

    Para cada línea de venta busca el acuerdo de su cliente-artículo-mes y
    calcula ajuste = desc_kg × kilos_cargo, que se RESTA de costo_unitario.
    Se usa kilos_cargo (solo lo COBRADO) y no kilos: los kilos bonificados
    no llevan costo, así que tampoco corresponde descontarles el acuerdo
    (mismo criterio que el Excel comercial). Las notas de crédito tienen
    kilos negativos, así que el ajuste se revierte solo en las devoluciones.
    Sin acuerdo -> ajuste 0 (costo intacto).

    Agrega la columna 'ajuste_mccain' para auditar. CM y CM% no se tocan acá:
    toda la app los deriva de costo_unitario, así que quedan bien solos.
    """
    if acuerdos is None:
        acuerdos = cargar_acuerdos()
    df = df_ventas.copy()
    if "kilos_cargo" not in df.columns:
        # Parquet guardado con la lógica vieja: re-derivar desde las columnas
        # crudas (siempre viajan en el parquet) para no depender de re-bajar.
        df = recalcular_costo(df)
    if acuerdos.empty:
        df["ajuste_mccain"] = 0.0
        return df

    df["_anio"] = df["fechaComprobate"].dt.year
    df["_mes"] = df["fechaComprobate"].dt.month
    ac = acuerdos.rename(columns={"anio": "_anio", "mes": "_mes"})
    df = df.merge(ac, on=["_anio", "_mes", "idCliente", "idArticulo"], how="left")
    df["desc_kg"] = df["desc_kg"].fillna(0.0)
    df["ajuste_mccain"] = df["desc_kg"] * df["kilos_cargo"]
    df["costo_unitario"] = df["costo_unitario"] - df["ajuste_mccain"]
    return df.drop(columns=["_anio", "_mes", "desc_kg"])


# ---------------------------------------------------------------------------
# 3bis) Metas de venta en KILOS (canal → proveedor → vendedor)
# ---------------------------------------------------------------------------
# Reemplaza los Excel de "Cierre / Meta" que se armaban a mano. El objetivo se
# carga desde la solapa "Metas" del tablero y se guarda acá, con el mismo
# mecanismo de upsert que los acuerdos.
#
# ESTRUCTURA (definida con la gestión comercial): se planifica EMPEZANDO POR
# EL CANAL, porque cada supervisor responde por el rendimiento de su canal.
# Recién después se abre por proveedor/línea y se reparte entre vendedores:
#
#     canal  →  proveedor / línea  →  vendedor
#
# Eso se modela con dos ejes en la misma tabla:
#
#   tipo   'objetivo'     meta del mes, se puede reajustar durante el mes.
#          'presupuesto'  lo que se estimó a principio de año para ese mes.
#                         Es la línea base; NO se toca durante el año.
#   nivel  'canal'        total del canal. Se CARGA a mano (no se deriva),
#                         para poder validar que la apertura cierre contra él.
#          'proveedor'    apertura por marca/línea dentro del canal.
#          'vendedor'     reparto entre los vendedores del canal.
#
# El nivel 'vendedor' es canal × vendedor (no canal × proveedor × vendedor):
# la grilla de tres ejes se vuelve inmanejable mes a mes y el control que se
# pidió (que la suma de los vendedores cierre contra el canal) se cumple
# igual. Si más adelante hace falta, se agrega un nivel 'proveedor_vendedor'
# sin tocar el esquema.
#
# No hay metas por cliente: es un nivel de desagregación demasiado fino para
# administrar. Los clientes aparecen en el análisis de ventas, sin objetivo.
#
# La meta SIEMPRE está en kilos. No se guardan porcentajes ni proyecciones:
# esos se derivan al leer con seguimiento_metas(), para que se recalculen
# solos a medida que entran ventas nuevas.

METAS_PATH = os.path.join(DATA_DIR, "metas.parquet")
METAS_HIST_PATH = os.path.join(DATA_DIR, "metas_historial.parquet")

METAS_COLS = ["anio_mes", "tipo", "nivel", "dsCanalMkt", "marca_linea",
              "dsVendedor", "meta_kg", "fecha_carga"]

METAS_TIPOS = ("objetivo", "presupuesto")
METAS_NIVELES = ("canal", "proveedor", "vendedor")

# Columna que identifica la fila dentro del canal, según el nivel. En 'canal'
# no hay apertura: las dos columnas de etiqueta van vacías.
METAS_ETIQUETA = {"canal": None, "proveedor": "marca_linea", "vendedor": "dsVendedor"}

# Clave lógica de una meta. Todo el upsert y la normalización giran alrededor
# de esto.
METAS_CLAVE = ["anio_mes", "tipo", "nivel", "dsCanalMkt", "marca_linea", "dsVendedor"]


def _metas_vacio():
    """DataFrame vacío con el esquema y los dtypes correctos."""
    d = pd.DataFrame(columns=METAS_COLS)
    d["meta_kg"] = d["meta_kg"].astype("float64")
    d["fecha_carga"] = pd.to_datetime(d["fecha_carga"])
    return d


def _concat_metas(*partes):
    """Concatena las tablas de metas dejando los dtypes fijos.

    Se descartan los DataFrames vacíos y se fuerzan las columnas al tipo que
    corresponde ANTES de concatenar. pandas avisa (FutureWarning) cuando en el
    concat hay columnas enteras en NA —pasa siempre que las metas viejas no
    tienen fecha_carga— porque en el futuro va a inferir los dtypes distinto:
    como acá el tipo se impone a mano, ese cambio no nos afecta y el aviso se
    silencia solo para esta operación.
    """
    vivas = []
    for p in partes:
        if p is None or len(p) == 0:
            continue
        d = p.copy()
        for c in METAS_COLS:
            if c not in d.columns:
                d[c] = np.nan
        for c in ["anio_mes", "tipo", "nivel", "dsCanalMkt", "marca_linea",
                  "dsVendedor"]:
            d[c] = d[c].astype(str).str.strip().replace(
                {"nan": "", "None": ""})
        d["meta_kg"] = pd.to_numeric(d["meta_kg"], errors="coerce").fillna(0.0)
        d["fecha_carga"] = pd.to_datetime(d["fecha_carga"], errors="coerce")
        vivas.append(d[METAS_COLS])

    if not vivas:
        return _metas_vacio()
    if len(vivas) == 1:
        return vivas[0]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        return pd.concat(vivas, ignore_index=True)


def _migrar_metas(df):
    """Lleva el parquet viejo (anio_mes, dsCanalMkt, marca_linea, meta_kg) al
    esquema nuevo. Todo lo que existía era el objetivo mensual abierto por
    proveedor, así que se etiqueta como tipo='objetivo', nivel='proveedor'.
    Es idempotente: si el archivo ya tiene las columnas nuevas, no toca nada."""
    d = df.copy()
    if "tipo" not in d.columns:
        d["tipo"] = "objetivo"
    if "nivel" not in d.columns:
        # Sin la columna 'nivel' toda fila con marca es apertura por proveedor.
        d["nivel"] = np.where(
            d.get("marca_linea", pd.Series(index=d.index, dtype=object))
            .astype(str).str.strip().isin(["", "nan", "None"]),
            "canal", "proveedor")
    if "dsVendedor" not in d.columns:
        d["dsVendedor"] = ""
    if "fecha_carga" not in d.columns:
        d["fecha_carga"] = pd.NaT
    return d


def cargar_metas(path=METAS_PATH):
    """Metas cargadas, en el esquema nuevo. DataFrame vacío (con columnas) si
    todavía no hay. Migra al vuelo el formato viejo, así el tablero sigue
    andando con el parquet que ya está en disco."""
    if not os.path.exists(path):
        return _metas_vacio()
    df = _migrar_metas(pd.read_parquet(path))
    for c in METAS_COLS:
        if c not in df.columns:
            df[c] = np.nan
    df = df[METAS_COLS].copy()
    for c in ["anio_mes", "tipo", "nivel", "dsCanalMkt", "marca_linea", "dsVendedor"]:
        df[c] = df[c].astype(str).str.strip().replace({"nan": "", "None": ""})
    df["meta_kg"] = pd.to_numeric(df["meta_kg"], errors="coerce").fillna(0.0)
    df["fecha_carga"] = pd.to_datetime(df["fecha_carga"], errors="coerce")
    return df


def normalizar_metas(df, tipo=None, nivel=None):
    """Limpia lo que viene de la grilla editable: normaliza textos, completa
    tipo/nivel si vienen por parámetro, blanquea las columnas de etiqueta que
    no corresponden al nivel, suma duplicados y tira las metas en cero (una
    meta de 0 kg equivale a no tener meta cargada).

    Descarta las filas sin la etiqueta que el nivel exige: sin marca en
    'proveedor', sin vendedor en 'vendedor'. En 'canal' no hace falta ninguna.
    """
    if df is None or len(df) == 0:
        return _metas_vacio()
    d = df.copy()
    for c in METAS_COLS:
        if c not in d.columns:
            d[c] = np.nan
    if tipo is not None:
        d["tipo"] = tipo
    if nivel is not None:
        d["nivel"] = nivel
    d = d[METAS_COLS]
    for c in ["anio_mes", "tipo", "nivel", "dsCanalMkt", "marca_linea", "dsVendedor"]:
        d[c] = d[c].astype(str).str.strip().replace({"nan": "", "None": ""})
    d["tipo"] = d["tipo"].str.lower().replace({"": "objetivo"})
    d["nivel"] = d["nivel"].str.lower().replace({"": "proveedor"})
    d["meta_kg"] = pd.to_numeric(d["meta_kg"], errors="coerce").fillna(0.0)
    d["fecha_carga"] = pd.to_datetime(d["fecha_carga"], errors="coerce")

    d = d[d["tipo"].isin(METAS_TIPOS) & d["nivel"].isin(METAS_NIVELES)]
    d = d[d["dsCanalMkt"] != ""].copy()   # .copy(): abajo se asigna con .loc

    # Cada nivel usa una sola columna de etiqueta; las demás se blanquean para
    # que la clave no se ensucie con restos de otra grilla.
    for niv, col in METAS_ETIQUETA.items():
        m = d["nivel"] == niv
        for otra in ("marca_linea", "dsVendedor"):
            if otra != col:
                d.loc[m, otra] = ""
        if col is not None:
            d = d[~(m & (d[col] == ""))].copy()

    d = d[d["meta_kg"] > 0]
    if d.empty:
        return _metas_vacio()
    return (
        d.groupby(METAS_CLAVE, as_index=False)
        .agg(meta_kg=("meta_kg", "sum"), fecha_carga=("fecha_carga", "max"))
        .sort_values(["anio_mes", "tipo", "nivel", "dsCanalMkt", "meta_kg"],
                     ascending=[True, True, True, True, False])
        .reset_index(drop=True)
    )


def cargar_historial_metas(path=METAS_HIST_PATH):
    """Log append-only de cada guardado. Sirve para mostrar 'objetivo original
    vs. objetivo vigente' cuando la meta del mes se reajusta a mitad de mes."""
    if not os.path.exists(path):
        return _metas_vacio()
    df = pd.read_parquet(path)
    for c in METAS_COLS:
        if c not in df.columns:
            df[c] = np.nan
    df = df[METAS_COLS].copy()
    df["meta_kg"] = pd.to_numeric(df["meta_kg"], errors="coerce").fillna(0.0)
    df["fecha_carga"] = pd.to_datetime(df["fecha_carga"], errors="coerce")
    return df


def _append_historial(snapshot, path=METAS_HIST_PATH):
    """Agrega el snapshot recién guardado al log. Nunca borra: si el archivo
    está corrupto o ilegible se saltea, porque el historial es informativo y
    no debe hacer fallar el guardado de la meta."""
    if snapshot is None or snapshot.empty:
        return
    try:
        prev = cargar_historial_metas(path)
        total = _concat_metas(prev, snapshot)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = path + ".tmp"
        total.to_parquet(tmp, index=False)
        os.replace(tmp, path)
    except Exception:
        pass


def upsert_metas(df_nuevas, anio_mes, canales=None, tipo="objetivo",
                 nivel="proveedor", path=METAS_PATH, historial=True,
                 path_historial=METAS_HIST_PATH):
    """UPSERT POR (mes, tipo, nivel, canal): borra del almacén esas
    combinaciones y las reemplaza por `df_nuevas`. Si `canales` viene dado,
    esos canales se reescriben aunque queden sin filas (así borrar una meta en
    la grilla efectivamente la borra).

    Acotar la clave a (tipo, nivel) además del canal es lo que permite guardar
    la grilla de vendedores sin pisar la de proveedores del mismo canal y mes.

    `anio_mes` puede ser un mes o una lista de meses (el presupuesto anual se
    guarda de una, los 12 meses juntos, en una sola escritura).

    Escritura atómica e idempotente. Devuelve la tabla completa resultante.
    """
    ahora = pd.Timestamp.now().floor("s")
    meses = ([str(anio_mes)] if isinstance(anio_mes, str)
             else [str(x) for x in anio_mes])
    nuevas = normalizar_metas(df_nuevas, tipo=tipo, nivel=nivel)
    nuevas["fecha_carga"] = ahora
    actual = cargar_metas(path)

    if canales is None:
        canales = sorted(nuevas["dsCanalMkt"].unique()) if not nuevas.empty else []
    canales = {str(c).strip() for c in canales}

    if not actual.empty and canales:
        borrar = (
            (actual["anio_mes"].astype(str).isin(meses))
            & (actual["tipo"].astype(str) == str(tipo))
            & (actual["nivel"].astype(str) == str(nivel))
            & (actual["dsCanalMkt"].astype(str).str.strip().isin(canales))
        )
        actual = actual[~borrar]

    # OJO: normalizar_metas() sin tipo/nivel, para no reetiquetar las filas
    # que ya estaban guardadas de otros niveles.
    total = normalizar_metas(_concat_metas(actual, nuevas))

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    total.to_parquet(tmp, index=False)
    os.replace(tmp, path)

    if historial:
        _append_historial(nuevas, path=path_historial)
    return total


def filtrar_metas(metas, anio_mes=None, tipo=None, nivel=None, canal=None,
                  canales=None):
    """Corte de la tabla de metas por los ejes de siempre. Devuelve una copia."""
    m = cargar_metas() if metas is None else metas.copy()
    if m.empty:
        return _metas_vacio()
    if anio_mes is not None:
        m = m[m["anio_mes"].astype(str) == str(anio_mes)]
    if tipo is not None:
        m = m[m["tipo"].astype(str) == str(tipo)]
    if nivel is not None:
        m = m[m["nivel"].astype(str) == str(nivel)]
    if canal is not None:
        m = m[m["dsCanalMkt"].astype(str).str.strip() == str(canal).strip()]
    if canales is not None:
        cs = {str(c).strip() for c in canales}
        m = m[m["dsCanalMkt"].astype(str).str.strip().isin(cs)]
    return m.reset_index(drop=True)


def total_meta(metas, anio_mes, tipo, nivel, canal=None, canales=None):
    """Suma de kilos de un corte. 0.0 si no hay nada cargado."""
    m = filtrar_metas(metas, anio_mes=anio_mes, tipo=tipo, nivel=nivel,
                      canal=canal, canales=canales)
    return float(m["meta_kg"].sum()) if not m.empty else 0.0


def metas_original_vs_vigente(anio_mes, tipo, nivel, canal, historial=None,
                              path=METAS_HIST_PATH):
    """Primer valor cargado vs. último, para un (mes, tipo, nivel, canal).

    El objetivo mensual se reajusta a propósito (si julio vendió de más porque
    los clientes adelantaron compras, agosto baja). Esto deja ver ese ajuste en
    vez de que el número original desaparezca al pisarse.

    Devuelve dict con original_kg / vigente_kg / fecha_original / fecha_vigente
    / n_cargas, o None si el historial no tiene nada de ese corte.
    """
    h = cargar_historial_metas(path) if historial is None else historial.copy()
    h = filtrar_metas(h, anio_mes=anio_mes, tipo=tipo, nivel=nivel, canal=canal)
    if h.empty or h["fecha_carga"].isna().all():
        return None
    tot = (h.dropna(subset=["fecha_carga"])
           .groupby("fecha_carga", as_index=False)["meta_kg"].sum()
           .sort_values("fecha_carga"))
    if tot.empty:
        return None
    return {
        "original_kg": float(tot["meta_kg"].iloc[0]),
        "vigente_kg": float(tot["meta_kg"].iloc[-1]),
        "fecha_original": tot["fecha_carga"].iloc[0],
        "fecha_vigente": tot["fecha_carga"].iloc[-1],
        "n_cargas": int(len(tot)),
    }


def dias_habiles(desde, hasta, con_sabado=True, feriados=None):
    """Cantidad de días de venta entre dos fechas (ambas inclusive).
    Por defecto cuenta lunes a sábado (no se factura los domingos). Es la base
    de la proyección a fin de mes; en el tablero el número se puede pisar a
    mano si el mes tuvo feriados o cierres.

    `feriados`: colección de dt.date que no se cuentan (no se factura)."""
    if desde is None or hasta is None or hasta < desde:
        return 0
    tope = 5 if con_sabado else 4  # weekday(): lunes=0 ... domingo=6
    fer = set(feriados or ())
    dias = 0
    d = desde
    while d <= hasta:
        if d.weekday() <= tope and d not in fer:
            dias += 1
        d += dt.timedelta(days=1)
    return dias


# --- Días de facturación de Food Service -----------------------------------
# Food Service NO factura todos los días. Cada vendedor está en la calle y
# vuelca los pedidos en dos a cuatro días fijos de la semana, así que proyectar
# ese canal contra días hábiles lo distorsiona: si el mes arranca un martes ya
# se "comió" un día de facturación, y una venta del 31 puede caer cargada el 3
# del mes siguiente. RETAIL, GRANJAS y MAYORISTAS facturan todos los días y
# siguen proyectando con dias_habiles().
#
# Es un PARCHE consciente mientras se ordena el proceso de facturación de Food
# (se les va a proponer facturar en el momento). Refleja los días declarados
# por Gaven en agosto 2026: si cambian, se cambian acá y nada más.
#
# weekday(): lunes=0, martes=1, miércoles=2, jueves=3, viernes=4, sábado=5.
CANAL_DIAS_FACTURACION = "FOOD SERVICE"

DIAS_FACTURACION = {
    "AVETTA SANCHEZ, MARIA NOELIA": (0, 2, 4),     # lunes, miércoles, viernes
    "BALLESTEROS, LAURA":           (0, 1, 3, 4),  # lunes, martes, jueves, viernes
    "CASTILLON AGUSTIN DAMIAN":     (0, 4),        # lunes, viernes
    "COLOMBO, CARLOS":              (0, 3),        # lunes, jueves
    "MORENO GERMAN":                (0, 1, 3),     # lunes, martes, jueves
}

DIAS_SEMANA_NOMBRE = ("lunes", "martes", "miércoles", "jueves", "viernes",
                      "sábado", "domingo")


def _norm_nombre(s):
    """Nombre comparable: mayúsculas, sin espacios de más."""
    return re.sub(r"\s+", " ", str(s).upper().strip())


def dias_facturacion_vendedor(canal, vendedor):
    """Días de la semana en que factura un vendedor.

    Devuelve None cuando no corresponde aplicar la lógica: o el canal no es
    Food Service (el resto factura todos los días), o el vendedor todavía no
    tiene los días declarados. En ambos casos la proyección se cae a
    dias_habiles(), que es el comportamiento de siempre.

    Importante que dependa del canal y no solo del nombre: MORENO GERMAN tiene
    ventas sueltas en GRANJAS y RETAIL, y ahí no factura en días fijos.
    """
    if _norm_nombre(canal) != _norm_nombre(CANAL_DIAS_FACTURACION):
        return None
    tabla = {_norm_nombre(k): v for k, v in DIAS_FACTURACION.items()}
    return tabla.get(_norm_nombre(vendedor))


def contar_dias_facturacion(desde, hasta, dias_semana, feriados=None):
    """Cuántos días de facturación hay entre dos fechas (ambas inclusive)."""
    if desde is None or hasta is None or hasta < desde:
        return 0
    dias = {int(d) for d in (dias_semana or ())}
    if not dias:
        return 0
    fer = set(feriados or ())
    n = 0
    d = desde
    while d <= hasta:
        if d.weekday() in dias and d not in fer:
            n += 1
        d += dt.timedelta(days=1)
    return n


def dias_venta(canal, vendedor, desde, corte, hasta, feriados=None):
    """(días transcurridos, días totales) de venta de un vendedor en el mes.

    Food Service cuenta días de facturación del vendedor; el resto de los
    canales, días hábiles de lunes a sábado.
    """
    dias = dias_facturacion_vendedor(canal, vendedor)
    if dias is None:
        return (dias_habiles(desde, corte, feriados=feriados),
                max(dias_habiles(desde, hasta, feriados=feriados), 1))
    return (contar_dias_facturacion(desde, corte, dias, feriados=feriados),
            max(contar_dias_facturacion(desde, hasta, dias, feriados=feriados), 1))


def factor_proyeccion(canal, vendedor, desde, corte, hasta, feriados=None):
    """Cuánto hay que escalar el avance de un vendedor para proyectarlo a fin
    de mes. 1.0 si el mes ya cerró o si todavía no hay días transcurridos."""
    pas, tot = dias_venta(canal, vendedor, desde, corte, hasta,
                          feriados=feriados)
    if pas > 0 and tot > 0 and pas < tot:
        return tot / pas
    return 1.0


def factor_proyeccion_ponderado(df, desde, corte, hasta, feriados=None,
                                col_peso="kilos"):
    """Factor de proyección único para un conjunto de ventas ya filtrado,
    ponderado por el peso de cada vendedor.

    Es la versión escalar de _proyeccion_por_nivel(): sirve donde hay que
    proyectar métricas que no son kilos (facturación, unidades) y no se puede
    trabajar fila por fila. Equivale a "la proyección total es la suma de las
    proyecciones de cada vendedor", que es lo mismo que pide el seguimiento de
    metas, así que los dos números del tablero cierran entre sí.

    Devuelve (factor, proyectar). `proyectar` es False si no hay con qué.
    """
    if df is None or len(df) == 0:
        return 1.0, False
    if any(c not in df.columns for c in ("dsCanalMkt", "dsVendedor", col_peso)):
        return 1.0, False

    g = df[["dsCanalMkt", "dsVendedor", col_peso]].copy()
    g["dsCanalMkt"] = g["dsCanalMkt"].astype(str).str.strip()
    g["dsVendedor"] = g["dsVendedor"].astype(str).str.strip()
    g[col_peso] = pd.to_numeric(g[col_peso], errors="coerce").fillna(0.0)
    g = g.groupby(["dsCanalMkt", "dsVendedor"], as_index=False)[col_peso].sum()
    g = g[g[col_peso] > 0]
    if g.empty:
        return 1.0, False

    base = float(g[col_peso].sum())
    proyectado = sum(
        w * factor_proyeccion(c, v, desde, corte, hasta, feriados=feriados)
        for c, v, w in g.itertuples(index=False, name=None)
    )
    if base <= 0 or proyectado <= 0:
        return 1.0, False
    factor = proyectado / base
    return factor, factor > 1.0


def vendedores_sin_dias_facturacion(df_ventas):
    """Vendedores de Food Service con ventas pero sin días declarados.

    El sistema AVISA, no bloquea: esos vendedores proyectan con días hábiles
    (lunes a sábado), que es lo que se hacía antes, y quedan listados para
    pedirle los días a Gaven.
    """
    if df_ventas is None or len(df_ventas) == 0:
        return []
    if "dsCanalMkt" not in df_ventas.columns or "dsVendedor" not in df_ventas.columns:
        return []
    d = df_ventas
    canal = d["dsCanalMkt"].astype(str).map(_norm_nombre)
    food = d[canal == _norm_nombre(CANAL_DIAS_FACTURACION)]
    if food.empty:
        return []
    faltan = {
        str(v).strip() for v in food["dsVendedor"].dropna().unique()
        if dias_facturacion_vendedor(CANAL_DIAS_FACTURACION, v) is None
    }
    return sorted(faltan - {"", "nan", "None"})


def etiqueta_dias_facturacion(vendedor, canal=CANAL_DIAS_FACTURACION):
    """'lunes, miércoles y viernes' — para mostrar en el tablero."""
    dias = dias_facturacion_vendedor(canal, vendedor)
    if not dias:
        return ""
    nombres = [DIAS_SEMANA_NOMBRE[d] for d in sorted(dias)]
    if len(nombres) == 1:
        return nombres[0]
    return ", ".join(nombres[:-1]) + " y " + nombres[-1]


def kilos_por_canal_marca(df_ventas):
    """Kilos agregados por canal × marca/línea (base del seguimiento)."""
    cols = pd.DataFrame(columns=["dsCanalMkt", "marca_linea", "kilos"])
    if df_ventas is None or df_ventas.empty:
        return cols
    d = df_ventas if "marca_linea" in df_ventas.columns else agregar_marca_linea(df_ventas)
    if "dsCanalMkt" not in d.columns:
        return cols
    g = d.copy()
    g["dsCanalMkt"] = g["dsCanalMkt"].astype(str).str.strip()
    g["marca_linea"] = g["marca_linea"].astype(str).str.strip()
    return (
        g.groupby(["dsCanalMkt", "marca_linea"], as_index=False)["kilos"]
        .sum()
    )


def kilos_por_canal_vendedor(df_ventas):
    """Kilos agregados por canal × vendedor (base del seguimiento del nivel
    'vendedor'). Mismo criterio que kilos_por_canal_marca()."""
    cols = pd.DataFrame(columns=["dsCanalMkt", "dsVendedor", "kilos"])
    if df_ventas is None or df_ventas.empty:
        return cols
    if "dsCanalMkt" not in df_ventas.columns or "dsVendedor" not in df_ventas.columns:
        return cols
    g = df_ventas.copy()
    g["dsCanalMkt"] = g["dsCanalMkt"].astype(str).str.strip()
    g["dsVendedor"] = g["dsVendedor"].astype(str).str.strip()
    return (
        g.groupby(["dsCanalMkt", "dsVendedor"], as_index=False)["kilos"]
        .sum()
    )


def kilos_por_canal(df_ventas):
    """Kilos totales por canal (base del seguimiento del nivel 'canal')."""
    cols = pd.DataFrame(columns=["dsCanalMkt", "kilos"])
    if df_ventas is None or df_ventas.empty or "dsCanalMkt" not in df_ventas.columns:
        return cols
    g = df_ventas.copy()
    g["dsCanalMkt"] = g["dsCanalMkt"].astype(str).str.strip()
    return g.groupby(["dsCanalMkt"], as_index=False)["kilos"].sum()


def kilos_por_mes_canal(df_ventas):
    """Kilos por mes (YYYY-MM) × canal. Es la referencia histórica que se
    muestra al cargar el presupuesto anual: cuánto se vendió realmente en ese
    mismo mes el año pasado."""
    cols = pd.DataFrame(columns=["anio_mes", "dsCanalMkt", "kilos"])
    if df_ventas is None or df_ventas.empty:
        return cols
    if "dsCanalMkt" not in df_ventas.columns or "fechaComprobate" not in df_ventas.columns:
        return cols
    g = df_ventas.copy()
    g["anio_mes"] = pd.to_datetime(
        g["fechaComprobate"], errors="coerce").dt.strftime("%Y-%m")
    g["dsCanalMkt"] = g["dsCanalMkt"].astype(str).str.strip()
    g = g[g["anio_mes"].notna()]
    return g.groupby(["anio_mes", "dsCanalMkt"], as_index=False)["kilos"].sum()


def _kilos_por_nivel(df_ventas, nivel):
    """Agregación de ventas al grano que corresponde al nivel de la meta."""
    if nivel == "vendedor":
        return kilos_por_canal_vendedor(df_ventas)
    if nivel == "canal":
        return kilos_por_canal(df_ventas)
    return kilos_por_canal_marca(df_ventas)


def _proyeccion_por_nivel(df_mes, nivel, desde, corte, hasta, feriados=None):
    """Proyección a fin de mes calculada SIEMPRE al grano de vendedor y recién
    después agregada al nivel pedido.

    Esto es lo que mantiene consistentes los tres niveles: la proyección de un
    canal es la suma de la de sus vendedores, y la de un proveedor es la suma
    de lo que proyectan los vendedores que lo venden. Si cada nivel se
    proyectara con un único factor global, canal ≠ Σ proveedores ≠ Σ
    vendedores en cuanto un canal tiene vendedores con distintos días de
    facturación.

    Devuelve DataFrame vacío si no se puede llegar al grano de vendedor; el
    que llama se cae a la proyección global de siempre.
    """
    etiqueta = METAS_ETIQUETA.get(nivel, "marca_linea")
    llaves = ["dsCanalMkt"] + ([etiqueta] if etiqueta else [])
    vacio = pd.DataFrame(columns=llaves + ["proyeccion_kg"])

    if df_mes is None or len(df_mes) == 0:
        return vacio

    d = df_mes
    if etiqueta == "marca_linea" and "marca_linea" not in d.columns:
        d = agregar_marca_linea(d)

    grano = list(dict.fromkeys(llaves + ["dsVendedor"]))
    if any(c not in d.columns for c in grano + ["kilos"]):
        return vacio

    g = d[grano + ["kilos"]].copy()
    for c in grano:
        g[c] = g[c].astype(str).str.strip()
    g = g.groupby(grano, as_index=False)["kilos"].sum()
    if g.empty:
        return vacio

    # Un factor por (canal, vendedor): son pocos pares, se calculan una vez.
    pares = g[["dsCanalMkt", "dsVendedor"]].drop_duplicates()
    factores = {
        (c, v): factor_proyeccion(c, v, desde, corte, hasta, feriados=feriados)
        for c, v in pares.itertuples(index=False, name=None)
    }
    g["proyeccion_kg"] = [
        k * factores[(c, v)]
        for k, c, v in zip(g["kilos"], g["dsCanalMkt"], g["dsVendedor"])
    ]
    return g.groupby(llaves, as_index=False)["proyeccion_kg"].sum()


def dias_venta_resumen(df_mes, desde, corte, hasta, canales=None,
                       feriados=None):
    """Días de venta transcurridos y totales 'promedio' de lo que se está
    mirando, ponderados por kilos de cada vendedor.

    Sirve solo para los textos del tablero ("día 6 de 13 de venta", "faltan
    X kg → Y kg/día"). La proyección NO usa esto: se calcula vendedor por
    vendedor. Con un canal que factura todos los días el promedio da
    exactamente dias_habiles(), así que no cambia nada fuera de Food.

    Devuelve (pasados, totales, mixto) — `mixto` avisa si en la vista hay
    vendedores con días de facturación propios.
    """
    pas_def = dias_habiles(desde, corte, feriados=feriados)
    tot_def = max(dias_habiles(desde, hasta, feriados=feriados), 1)

    if df_mes is None or len(df_mes) == 0:
        return pas_def, tot_def, False
    if any(c not in df_mes.columns for c in ("dsCanalMkt", "dsVendedor", "kilos")):
        return pas_def, tot_def, False

    g = df_mes[["dsCanalMkt", "dsVendedor", "kilos"]].copy()
    g["dsCanalMkt"] = g["dsCanalMkt"].astype(str).str.strip()
    g["dsVendedor"] = g["dsVendedor"].astype(str).str.strip()
    if canales:
        cs = {str(c).strip() for c in canales}
        g = g[g["dsCanalMkt"].isin(cs)]
    g = g.groupby(["dsCanalMkt", "dsVendedor"], as_index=False)["kilos"].sum()
    g = g[g["kilos"] > 0]
    if g.empty:
        return pas_def, tot_def, False

    mixto = any(
        dias_facturacion_vendedor(c, v) is not None
        for c, v in g[["dsCanalMkt", "dsVendedor"]].itertuples(index=False, name=None)
    )
    dias = [dias_venta(c, v, desde, corte, hasta, feriados=feriados)
            for c, v in g[["dsCanalMkt", "dsVendedor"]].itertuples(index=False, name=None)]
    peso = g["kilos"].to_numpy(dtype="float64")
    total_peso = float(peso.sum())
    if total_peso <= 0:
        return pas_def, tot_def, mixto

    pas = sum(p * w for (p, _), w in zip(dias, peso)) / total_peso
    tot = sum(t * w for (_, t), w in zip(dias, peso)) / total_peso
    return int(round(pas)), max(int(round(tot)), 1), mixto


def seguimiento_metas(df_mes, metas, anio_mes, dias_pasados=None,
                      dias_totales=None, df_mes_anterior=None, canales=None,
                      nivel="proveedor", tipo="objetivo",
                      desde=None, corte=None, hasta=None, feriados=None):
    """Arma la tabla de seguimiento: objetivo vs. avance real, con proyección
    a fin de mes, alcance % y comparación contra el mes anterior.

    - `df_mes`: ventas del mes de la meta (SIN los filtros globales, para que
      el seguimiento sea siempre el del canal completo).
    - `metas`: salida de cargar_metas() (se filtra por anio_mes/tipo/nivel acá).
    - `nivel`: 'canal', 'proveedor' o 'vendedor'. Define contra qué grano de
      ventas se compara la meta. El default es 'proveedor' para no romper a
      quien ya llamaba esta función con la firma vieja.
    - `desde` / `corte` / `hasta`: primer día del mes, último día CON datos y
      último día del mes. Si los tres vienen, la proyección se calcula
      vendedor por vendedor (Food Service contra sus días de facturación, el
      resto contra días hábiles) y se agrega al nivel pedido. Es la forma
      correcta y la que usa el tablero.
    - `dias_pasados` / `dias_totales`: modo viejo, un único factor global
      (avance / dias_pasados * dias_totales). Se usa solo como respaldo
      cuando no se pasan las fechas. Si el mes ya cerró la proyección es el
      avance.
    - Devuelve TODAS las filas con meta y también las que vendieron sin meta
      (meta 0), para que no se escape volumen del seguimiento.
    """
    etiqueta = METAS_ETIQUETA.get(nivel, "marca_linea")
    llaves = ["dsCanalMkt"] + ([etiqueta] if etiqueta else [])
    cols = llaves + ["meta_kg", "avance_kg", "proyeccion_kg", "alcance_pct",
                     "brecha_kg", "falta_kg", "mes_ant_kg", "var_ant_pct"]

    m = filtrar_metas(metas, anio_mes=anio_mes, tipo=tipo, nivel=nivel)
    m = (m[llaves + ["meta_kg"]] if not m.empty
         else pd.DataFrame(columns=llaves + ["meta_kg"]))

    real = _kilos_por_nivel(df_mes, nivel).rename(columns={"kilos": "avance_kg"})
    prev = _kilos_por_nivel(df_mes_anterior, nivel).rename(
        columns={"kilos": "mes_ant_kg"})

    t = m.merge(real, on=llaves, how="outer")
    t = t.merge(prev, on=llaves, how="left")
    if t.empty:
        return pd.DataFrame(columns=cols)

    if canales:
        canales = {str(c).strip() for c in canales}
        t = t[t["dsCanalMkt"].astype(str).str.strip().isin(canales)]
    if t.empty:
        return pd.DataFrame(columns=cols)

    for c in ["meta_kg", "avance_kg", "mes_ant_kg"]:
        t[c] = pd.to_numeric(t[c], errors="coerce").fillna(0.0)

    proy = None
    if desde is not None and corte is not None and hasta is not None:
        p = _proyeccion_por_nivel(df_mes, nivel, desde, corte, hasta,
                                  feriados=feriados)
        if not p.empty:
            proy = p

    if proy is not None:
        for c in llaves:
            t[c] = t[c].astype(str).str.strip()
        t = t.merge(proy, on=llaves, how="left")
        # Las filas con meta pero sin ventas no aparecen en la proyección:
        # su avance es 0 y proyectan 0.
        t["proyeccion_kg"] = pd.to_numeric(
            t["proyeccion_kg"], errors="coerce").fillna(t["avance_kg"])
    else:
        dp_ = float(dias_pasados or 0)
        dt_ = float(dias_totales or 0)
        if dp_ > 0 and dt_ > 0 and dp_ < dt_:
            t["proyeccion_kg"] = t["avance_kg"] / dp_ * dt_
        else:
            t["proyeccion_kg"] = t["avance_kg"]

    t["alcance_pct"] = np.where(
        t["meta_kg"] > 0, t["proyeccion_kg"] / t["meta_kg"] * 100, np.nan)
    t["brecha_kg"] = t["proyeccion_kg"] - t["meta_kg"]
    t["falta_kg"] = (t["meta_kg"] - t["avance_kg"]).clip(lower=0)
    t["var_ant_pct"] = np.where(
        t["mes_ant_kg"] > 0, (t["avance_kg"] / t["mes_ant_kg"] - 1) * 100, np.nan)

    return (t[cols]
            .sort_values(["meta_kg", "avance_kg"], ascending=False)
            .reset_index(drop=True))


# --- Controles de consistencia entre niveles -------------------------------
# La idea es que haya una lógica detrás de los números y no campos sueltos:
# lo que se abre por proveedor y lo que se reparte entre vendedores tiene que
# cerrar contra el total del canal. El sistema AVISA, no bloquea: hay que
# poder guardar a medio cargar y seguir mañana.

VALIDACION_COLS = ["control", "esperado_kg", "cargado_kg", "dif_kg", "dif_pct",
                   "estado", "detalle"]


def validar_metas(metas, anio_mes, canal, tipo="objetivo", tolerancia_pct=0.5,
                  comparar_presupuesto=True):
    """Chequea que la apertura de un canal cierre contra su total.

    Controles:
      1. Σ objetivos de proveedores  vs. objetivo del canal.
      2. Σ objetivos de vendedores   vs. objetivo del canal.
      3. Objetivo del mes            vs. presupuesto anual de ese mes
         (informativo: el objetivo mensual PUEDE desviarse del presupuesto,
         justamente porque se ajusta a la realidad reciente).

    `tolerancia_pct` es sobre el total del canal: diferencias de redondeo por
    debajo de eso no se marcan. Devuelve un DataFrame con una fila por control.
    """
    filas = []
    m = filtrar_metas(metas, anio_mes=anio_mes, canal=canal)

    tot_canal = total_meta(m, anio_mes, tipo, "canal", canal)
    tot_prov = total_meta(m, anio_mes, tipo, "proveedor", canal)
    tot_vend = total_meta(m, anio_mes, tipo, "vendedor", canal)

    tol = max(abs(tot_canal) * float(tolerancia_pct) / 100.0, 1.0)

    def _fila(control, esperado, cargado, detalle_ok, detalle_mal,
              detalle_falta):
        if esperado <= 0:
            return {
                "control": control, "esperado_kg": esperado,
                "cargado_kg": cargado, "dif_kg": np.nan, "dif_pct": np.nan,
                "estado": "falta", "detalle": detalle_falta,
            }
        dif = cargado - esperado
        pct = dif / esperado * 100 if esperado else np.nan
        ok = abs(dif) <= tol
        return {
            "control": control, "esperado_kg": esperado, "cargado_kg": cargado,
            "dif_kg": dif, "dif_pct": pct,
            "estado": "ok" if ok else "aviso",
            "detalle": detalle_ok if ok else detalle_mal.format(
                dif=abs(dif), pct=abs(pct),
                signo="de más" if dif > 0 else "de menos"),
        }

    filas.append(_fila(
        "Σ proveedores vs. objetivo del canal", tot_canal, tot_prov,
        "La apertura por proveedor cierra contra el total del canal.",
        "La apertura por proveedor da {dif:,.0f} kg {signo} ({pct:,.1f}%) "
        "que el objetivo del canal.",
        "Todavía no se cargó el objetivo total del canal, así que no hay "
        "contra qué validar la apertura por proveedor.",
    ))
    filas.append(_fila(
        "Σ vendedores vs. objetivo del canal", tot_canal, tot_vend,
        "El reparto entre vendedores cierra contra el total del canal.",
        "El reparto entre vendedores da {dif:,.0f} kg {signo} ({pct:,.1f}%) "
        "que el objetivo del canal.",
        "Todavía no se cargó el objetivo total del canal, así que no hay "
        "contra qué validar el reparto entre vendedores.",
    ))

    # Aviso extra: hay total de canal pero ninguna apertura cargada.
    if tot_canal > 0 and tot_prov == 0:
        filas[0]["estado"] = "falta"
        filas[0]["detalle"] = ("El canal tiene objetivo pero no está abierto "
                               "por proveedor.")
    if tot_canal > 0 and tot_vend == 0:
        filas[1]["estado"] = "falta"
        filas[1]["detalle"] = ("El canal tiene objetivo pero no está repartido "
                               "entre los vendedores.")

    if comparar_presupuesto and tipo == "objetivo":
        pres = total_meta(m, anio_mes, "presupuesto", "canal", canal)
        if pres > 0 and tot_canal > 0:
            dif = tot_canal - pres
            pct = dif / pres * 100
            filas.append({
                "control": "Objetivo del mes vs. presupuesto anual",
                "esperado_kg": pres, "cargado_kg": tot_canal,
                "dif_kg": dif, "dif_pct": pct, "estado": "info",
                "detalle": (
                    f"El objetivo del mes está {abs(pct):,.1f}% "
                    f"{'por encima' if dif > 0 else 'por debajo'} del "
                    "presupuesto. No es un error: el objetivo mensual se "
                    "ajusta a la realidad del negocio."),
            })
        elif tot_canal > 0:
            filas.append({
                "control": "Objetivo del mes vs. presupuesto anual",
                "esperado_kg": 0.0, "cargado_kg": tot_canal,
                "dif_kg": np.nan, "dif_pct": np.nan, "estado": "falta",
                "detalle": "No hay presupuesto anual cargado para este mes.",
            })

    return pd.DataFrame(filas, columns=VALIDACION_COLS)


def icono_validacion(estado):
    """Ícono del panel de controles de consistencia."""
    return {"ok": "✅", "aviso": "⚠️", "falta": "🔸", "info": "ℹ️"}.get(estado, "·")


def semaforo(alcance_pct, verde=100.0, amarillo=90.0):
    """Color del alcance proyectado: verde si llega a la meta, amarillo si
    queda cerca, rojo si está lejos. Sin meta cargada -> gris."""
    if alcance_pct is None or (isinstance(alcance_pct, float) and np.isnan(alcance_pct)):
        return "⚪"
    if alcance_pct >= verde:
        return "🟢"
    if alcance_pct >= amarillo:
        return "🟡"
    return "🔴"


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


def main(hasta=None):
    """`hasta` (dt.date, opcional): fecha de corte de los datos. Por defecto
    es hoy. Pasar una fecha anterior regenera el mes actual solo hasta esa
    fecha (p. ej. para publicar un tablero con corte a un día específico)."""
    cfg = cargar_credenciales()
    headers = login(cfg["base_url"], cfg["usuario"], cfg["password"])

    if hasta:
        print(f"*** Corte de datos: {hasta} (el mes actual se trae solo "
              f"hasta esa fecha) ***")

    # Mes actual + anterior SIEMPRE, más los meses de ANIO que falten en el
    # parquet (auto-backfill: solo la primera vez o si un mes quedó a medias).
    ventanas = ventanas_a_traer(hoy=hasta)
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
    import argparse

    parser = argparse.ArgumentParser(description="ETL de ventas (API Chess)")
    parser.add_argument(
        "--hasta", metavar="YYYY-MM-DD", default=None,
        help="Fecha de corte de los datos (default: hoy). Ej: --hasta 2026-07-25")
    args = parser.parse_args()

    hasta = dt.date.fromisoformat(args.hasta) if args.hasta else None
    main(hasta)
