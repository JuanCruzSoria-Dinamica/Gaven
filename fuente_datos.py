"""
fuente_datos.py
---------------
De dónde lee el panel: los parquets de data/ (como siempre) o la base de
Supabase ("Gaven Base").

La fuente se elige en .streamlit/secrets.toml:

    [datos]
    fuente = "supabase"        # o "parquet" (el valor por defecto)

    [supabase]
    conninfo = "host=db.zfvskslwjzoihrmmmguh.supabase.co port=5432 dbname=postgres user=panel_app sslmode=require"
    # password = "..."         # opcional: en el servidor la toma de /root/.pgpass

También se puede forzar con la variable de entorno GAVEN_FUENTE=parquet|supabase
(sirve para volver a los parquets sin tocar el secrets.toml).

En modo Supabase el panel lee las vistas del esquema `mart`, que ya traen el
costo final (acuerdos McCain + bonificación por canal), la marca/línea, la
familia y el tipo de producto. Por eso acá NO se vuelven a aplicar
aplicar_acuerdos / aplicar_bonificacion_canal / los lookups CSV.

Este módulo no importa Streamlit a nivel de funciones de lectura: recibe la
configuración ya resuelta, así se puede usar también desde scripts de prueba.
"""

import io
import os
import datetime as dt

import pandas as pd

FUENTES = ("parquet", "supabase")

# Columnas que se piden a mart.ventas_mat: las mismas que tenía el parquet de
# detalle, más las que la base ya trae calculadas. letra y serie entran para
# que dp.comprobante_id() separe las facturas A y B con el mismo número (igual
# que mart.serie_mensual). Las que el panel no usa (id_linea, id) quedan afuera
# para no gastar memoria.
COLS_VENTAS = [
    "dsEmpresa", "dsDocumento", "letra", "serie", "nrodoc", "fechaComprobate",
    "idCliente", "nombreCliente", "dsLocalidad", "dsProvincia", "region",
    "idVendedor", "dsVendedor",
    "idCanalMkt", "dsCanalMkt", "idSubcanalMkt", "dsSubcanalMKT",
    "idArticulo", "dsArticulo", "proveedor", "marca_linea", "familia",
    "tipo_producto", "Categoria",
    "cantidadesTotal", "peso", "pesoTotal", "unimedtotal", "unimedcargo",
    "precioUnitarioNeto", "subtotalNeto", "subtotalFinal", "preciocomprant",
    "kilos", "kilos_cargo", "bultos_cargo",
    "ajuste_mccain", "ajuste_bonif_canal", "costo_unitario",
]

COLS_SERIE = ["anio_mes", "dsCanalMkt", "dsSubcanalMKT", "dsVendedor", "region",
              "marca_linea", "kilos", "subtotalNeto", "costo", "cm",
              "clientes", "comprobantes"]

COLS_CARTERA = ["anio_mes", "dsCanalMkt", "dsSubcanalMKT", "dsVendedor",
                "region", "marca_linea", "idCliente", "idArticulo"]

COLS_METAS = ["anio_mes", "tipo", "nivel", "dsCanalMkt", "marca_linea",
              "dsVendedor", "meta_kg", "fecha_carga"]


# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

def resolver_fuente(secrets=None):
    """'parquet' o 'supabase'. Prioridad: variable de entorno GAVEN_FUENTE,
    después [datos] fuente del secrets.toml, después 'parquet'."""
    f = os.environ.get("GAVEN_FUENTE")
    if not f and secrets is not None:
        try:
            f = secrets["datos"]["fuente"]
        except Exception:
            f = None
    f = (f or "parquet").strip().lower()
    return f if f in FUENTES else "parquet"


def resolver_conninfo(secrets=None):
    """Cadena de conexión de psycopg para el usuario panel_app."""
    ci = os.environ.get("GAVEN_DB_CONNINFO")
    pwd = os.environ.get("GAVEN_DB_PASSWORD")
    if not ci and secrets is not None:
        try:
            sec = secrets["supabase"]
            ci = sec.get("conninfo")
            pwd = pwd or sec.get("password")
        except Exception:
            ci = None
    if not ci:
        raise RuntimeError(
            "Falta la conexión a Supabase: agregá [supabase] conninfo en "
            ".streamlit/secrets.toml (o la variable GAVEN_DB_CONNINFO).")
    if pwd and "password=" not in ci:
        ci = f"{ci} password={pwd}"
    return ci


def _conectar(conninfo):
    import psycopg  # import tardío: en modo parquet no hace falta instalarlo
    return psycopg.connect(conninfo, connect_timeout=15)


# ---------------------------------------------------------------------------
# Lectura
# ---------------------------------------------------------------------------

def _copy_a_df(conninfo, sql, **read_csv_kw):
    """Trae el resultado de `sql` con COPY ... TO STDOUT (CSV) y lo lee con
    pandas. Es mucho más liviano que fetchall(): no arma una tupla de Python
    por cada fila (con ~250 mil filas eso son cientos de MB transitorios)."""
    buf = io.BytesIO()
    with _conectar(conninfo) as con, con.cursor() as cur:
        with cur.copy(f"COPY ({sql}) TO STDOUT WITH (FORMAT csv, HEADER true)") as cp:
            for bloque in cp:
                buf.write(bloque)
    buf.seek(0)
    return pd.read_csv(buf, **read_csv_kw)


def _consulta(conninfo, sql, params=None):
    with _conectar(conninfo) as con, con.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def _q(col):
    return '"' + col + '"'


def leer_ventas(conninfo, anio):
    """Detalle de ventas del año `anio` desde mart.ventas_mat, con los mismos
    nombres y tipos de columna que el parquet de detalle ya procesado."""
    cols = ", ".join(_q(c) for c in COLS_VENTAS)
    sql = (f"select {cols} from mart.ventas_mat "
           f"where \"fechaComprobate\" >= date '{int(anio)}-01-01' "
           f"and \"fechaComprobate\" < date '{int(anio) + 1}-01-01'")
    texto = ["dsEmpresa", "dsDocumento", "letra", "serie", "nombreCliente", "dsLocalidad",
             "dsProvincia", "region", "dsVendedor", "dsCanalMkt",
             "dsSubcanalMKT", "dsArticulo", "proveedor", "marca_linea",
             "familia", "tipo_producto", "Categoria"]
    df = _copy_a_df(conninfo, sql, dtype={c: "object" for c in texto},
                    keep_default_na=False, na_values=[""])
    df["fechaComprobate"] = pd.to_datetime(df["fechaComprobate"])
    for c in ["nrodoc", "idCliente", "idVendedor", "idCanalMkt",
              "idSubcanalMkt", "idArticulo"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")
    for c in texto:
        df[c] = df[c].fillna("")
    return df


def leer_serie(conninfo):
    """Serie mensual agregada (mart.serie_mensual_mat), desde 2025."""
    cols = ", ".join(_q(c) for c in COLS_SERIE)
    df = _copy_a_df(conninfo, f"select {cols} from mart.serie_mensual_mat",
                    dtype={"anio_mes": "object"}, keep_default_na=False,
                    na_values=[""])
    for c in ["dsCanalMkt", "dsSubcanalMKT", "dsVendedor", "region", "marca_linea"]:
        df[c] = df[c].fillna("").astype(str)
    return df


def leer_cartera(conninfo):
    """Combinaciones mes × grano × cliente × SKU (mart.cartera_mat). Los ids
    quedan como texto, igual que en serie_cartera.parquet."""
    cols = ", ".join(_q(c) for c in COLS_CARTERA)
    df = _copy_a_df(conninfo, f"select {cols} from mart.cartera_mat",
                    dtype=str, keep_default_na=False)
    return df


def leer_metas(conninfo):
    """Metas vigentes en el mismo formato que dp.cargar_metas()."""
    df = _consulta(conninfo,
                   "select anio_mes, tipo, nivel, \"dsCanalMkt\", marca_linea, "
                   "\"dsVendedor\", meta_kg::float8 as meta_kg, fecha_carga "
                   "from mart.metas")
    return _normalizar_metas(df)


def leer_historial_metas(conninfo):
    """Historial de metas en el formato de dp.cargar_historial_metas(): cada
    alta o cambio, con la fecha en que se registró. Las bajas no cuentan como
    una carga."""
    df = _consulta(conninfo,
                   "select anio_mes, tipo, nivel, \"dsCanalMkt\", marca_linea, "
                   "\"dsVendedor\", meta_kg::float8 as meta_kg, "
                   "registrado_en as fecha_carga "
                   "from mart.metas_historial where operacion <> 'baja'")
    return _normalizar_metas(df)


def _normalizar_metas(df):
    if df.empty:
        return pd.DataFrame({c: pd.Series(dtype="object") for c in COLS_METAS})
    for c in ["anio_mes", "tipo", "nivel", "dsCanalMkt", "marca_linea", "dsVendedor"]:
        df[c] = df[c].fillna("").astype(str).str.strip()
    df["meta_kg"] = pd.to_numeric(df["meta_kg"], errors="coerce").fillna(0.0)
    # La base guarda timestamptz; el panel trabaja con hora argentina sin zona.
    f = pd.to_datetime(df["fecha_carga"], errors="coerce", utc=True)
    df["fecha_carga"] = f.dt.tz_convert("America/Argentina/Buenos_Aires").dt.tz_localize(None)
    return df[COLS_METAS].copy()


def leer_acuerdos(conninfo):
    """Acuerdos McCain en el formato de dp.cargar_acuerdos()."""
    df = _consulta(conninfo,
                   "select extract(year from mes)::int as anio, "
                   "extract(month from mes)::int as mes, id_cliente as \"idCliente\", "
                   "id_articulo as \"idArticulo\", desc_kg::float8 as desc_kg "
                   "from core.acuerdo_costo order by mes, id_cliente, id_articulo")
    return df


def leer_ipc(conninfo):
    """IPC del INDEC en el formato de ipc_indec.parquet (anio_mes, ipc)."""
    return _consulta(conninfo, "select anio_mes, ipc from mart.ipc order by anio_mes")


def leer_feriados(conninfo):
    """Feriados como frozenset de dt.date (mismo tipo que dp.FERIADOS)."""
    df = _consulta(conninfo, "select fecha from mart.feriados")
    return frozenset(pd.to_datetime(df["fecha"]).dt.date) if not df.empty else frozenset()


def leer_dias_facturacion(conninfo, hoy=None):
    """Días de facturación vigentes por código de vendedor, con la misma forma
    que dp.DIAS_FACTURACION: {id_vendedor: (0, 2, 4), ...}."""
    hoy = hoy or dt.date.today()
    df = _consulta(conninfo,
                   "select \"idVendedor\", dia_semana, vigente_desde, vigente_hasta "
                   "from mart.dias_facturacion")
    if df.empty:
        return {}
    # Se compara como Timestamp: con pandas 3, una columna de fechas toda
    # vacía (vigente_hasta = null) no se puede comparar contra un dt.date.
    hoy_ts = pd.Timestamp(hoy)
    desde = pd.to_datetime(df["vigente_desde"], errors="coerce")
    hasta = pd.to_datetime(df["vigente_hasta"], errors="coerce")
    vig = (desde <= hoy_ts) & (hasta.isna() | (hasta >= hoy_ts))
    df = df[vig.values]
    return {int(v): tuple(sorted(int(d) for d in g["dia_semana"]))
            for v, g in df.groupby("idVendedor")}


def leer_estado(conninfo):
    """Una fila de mart.ultima_actualizacion como dict."""
    df = _consulta(conninfo, "select * from mart.ultima_actualizacion")
    return {} if df.empty else df.iloc[0].to_dict()


# ---------------------------------------------------------------------------
# Escritura: metas y acuerdos (usuario panel_app)
# ---------------------------------------------------------------------------
# Mismo criterio que dp.upsert_metas / dp.upsert_acuerdos, pero en la base y en
# una sola transacción: si algo falla, no queda nada a medio guardar.

def _mes_date(anio_mes):
    """'2026-11' -> date(2026, 11, 1), como guarda la base el mes."""
    s = str(anio_mes).strip()
    return dt.date(int(s[:4]), int(s[5:7]), 1)


def version_metas(conninfo):
    """Cambia con cada alta, cambio o baja de una meta: el trigger de core.meta
    deja una fila en el historial por cada una. Sirve de clave de caché."""
    df = _consulta(conninfo, "select coalesce(max(id), 0) as v, count(*) as n "
                             "from mart.metas_historial")
    return f"{int(df['v'].iloc[0])}-{int(df['n'].iloc[0])}"


def guardar_metas(conninfo, df_nuevas, anio_mes, canales=None, tipo="objetivo",
                  nivel="proveedor", cargado_por=None):
    """UPSERT POR (mes, tipo, nivel, canal) en core.meta, igual que
    dp.upsert_metas: borra esas combinaciones y las reemplaza por `df_nuevas`.
    Si `canales` viene dado, esos canales se reescriben aunque queden sin filas
    (así borrar una meta en la grilla la borra).

    Borrar e insertar en la misma transacción hace que el historial (lo llena
    el trigger) quede con una "foto" completa de cada guardado, todas con la
    misma hora: es lo que espera dp.metas_original_vs_vigente.

    Los nombres de canal, marca/línea y vendedor se traducen a los ids de la
    base. Si alguno no existe, no se guarda nada y se levanta ValueError.
    Devuelve las metas vigentes completas (mismo formato que leer_metas)."""
    import data_pipeline as dp  # import tardío: dp no depende de este módulo

    meses = ([str(anio_mes)] if isinstance(anio_mes, str)
             else [str(x) for x in anio_mes])
    nuevas = dp.normalizar_metas(df_nuevas, tipo=tipo, nivel=nivel)
    if canales is None:
        canales = sorted(nuevas["dsCanalMkt"].unique()) if not nuevas.empty else []
    canales = {str(c).strip() for c in canales}

    with _conectar(conninfo) as con:
        with con.cursor() as cur:
            cur.execute("select nombre, id_canal from mart.dim_canal")
            id_canal = {str(n).strip(): int(i) for n, i in cur.fetchall()}
            cur.execute("select nombre, id from mart.dim_marca_linea")
            id_marca = {str(n).strip(): int(i) for n, i in cur.fetchall()}
            cur.execute("select nombre, id_persona from mart.dim_vendedor")
            id_persona = {str(n).strip(): int(i) for n, i in cur.fetchall()}

        filas, faltan = [], set()
        for r in nuevas.itertuples(index=False):
            c = id_canal.get(r.dsCanalMkt)
            if c is None:
                faltan.add(f"canal '{r.dsCanalMkt}'")
                continue
            m = p = None
            if r.nivel == "proveedor":
                m = id_marca.get(r.marca_linea)
                if m is None:
                    faltan.add(f"marca/línea '{r.marca_linea}'")
                    continue
            elif r.nivel == "vendedor":
                p = id_persona.get(r.dsVendedor)
                if p is None:
                    faltan.add(f"vendedor '{r.dsVendedor}'")
                    continue
            filas.append((_mes_date(r.anio_mes), r.tipo, r.nivel, c, m, p,
                          float(r.meta_kg), cargado_por))
        if faltan:
            raise ValueError("No se guardó nada: estos nombres no están en la "
                             "base: " + ", ".join(sorted(faltan)))

        ids_canales = [id_canal[c] for c in canales if c in id_canal]
        with con.transaction(), con.cursor() as cur:
            cur.execute(
                "delete from core.meta where mes = any(%s::date[]) "
                "and tipo = %s and nivel = %s and id_canal = any(%s::int[])",
                ([_mes_date(m) for m in meses], tipo, nivel, ids_canales))
            if filas:
                cur.executemany(
                    "insert into core.meta (mes, tipo, nivel, id_canal, "
                    "id_marca_linea, id_persona, meta_kg, cargado_por) "
                    "values (%s, %s, %s, %s, %s, %s, %s, %s)", filas)
    return leer_metas(conninfo)


def guardar_acuerdos(conninfo, df_nuevos, nombre_archivo=None, rol=None,
                     resumen=None):
    """UPSERT POR MES en core.acuerdo_costo, igual que dp.upsert_acuerdos: los
    meses que trae el archivo reemplazan a los cargados. Registra la carga en
    core.carga_archivo.

    La base exige que el cliente y el artículo existan y que el artículo tenga
    proveedor. Las filas que no cumplen se descartan (no se aplicarían a
    ninguna venta) y se devuelven para mostrarlas.

    No refresca las vistas: eso lo hace refrescar_vistas(), aparte, porque
    tarda alrededor de un minuto.

    Devuelve {"guardados": int, "descartados": DataFrame}."""
    import json
    from psycopg.types.json import Jsonb

    d = df_nuevos.copy()
    d["anio"] = pd.to_numeric(d["anio"], errors="coerce").astype("int64")
    d["mes"] = pd.to_numeric(d["mes"], errors="coerce").astype("int64")
    d["idCliente"] = pd.to_numeric(d["idCliente"], errors="coerce").astype("int64")
    d["idArticulo"] = pd.to_numeric(d["idArticulo"], errors="coerce").astype("int64")
    d["desc_kg"] = pd.to_numeric(d["desc_kg"], errors="coerce").astype(float)
    d["mes_d"] = [dt.date(int(a), int(m), 1) for a, m in zip(d["anio"], d["mes"])]
    meses = sorted(set(d["mes_d"]))

    with _conectar(conninfo) as con:
        with con.cursor() as cur:
            cur.execute("select id_articulo, id_proveedor from mart.dim_articulo")
            prov = {int(a): (int(p) if p is not None else None)
                    for a, p in cur.fetchall()}
            cur.execute("select id_cliente from mart.dim_cliente")
            clientes = {int(c) for (c,) in cur.fetchall()}

        d["prov_id"] = d["idArticulo"].map(prov)
        ok = d["idCliente"].isin(clientes) & d["prov_id"].notna()
        validos = d[ok]
        descartados = d.loc[~ok, ["anio", "mes", "idCliente", "idArticulo",
                                  "desc_kg"]].reset_index(drop=True)

        info = json.loads(json.dumps(resumen or {}, default=str))
        info["descartados_en_base"] = int(len(descartados))
        with con.transaction(), con.cursor() as cur:
            cur.execute(
                "insert into core.carga_archivo (tipo, nombre_archivo, rol, "
                "filas, resumen) values ('acuerdos', %s, %s, %s, %s) "
                "returning id",
                (nombre_archivo, rol, int(len(validos)), Jsonb(info)))
            id_carga = cur.fetchone()[0]
            cur.execute("delete from core.acuerdo_costo "
                        "where mes = any(%s::date[])", (meses,))
            if not validos.empty:
                cur.executemany(
                    "insert into core.acuerdo_costo (mes, id_cliente, "
                    "id_articulo, id_proveedor, desc_kg, id_carga) "
                    "values (%s, %s, %s, %s, %s, %s)",
                    [(r.mes_d, int(r.idCliente), int(r.idArticulo),
                      int(r.prov_id), float(r.desc_kg), id_carga)
                     for r in validos.itertuples(index=False)])
    return {"guardados": int(len(validos)), "descartados": descartados}


def refrescar_vistas(conninfo):
    """Recalcula las vistas materializadas (mart.refrescar(), ~1 minuto). Hace
    falta después de guardar acuerdos: el costo de mart.ventas_mat los usa."""
    with _conectar(conninfo) as con:
        con.execute("select mart.refrescar()")
