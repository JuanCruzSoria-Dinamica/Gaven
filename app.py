"""
app.py
------
Panel de ventas (Gaven). SOLO presentación.

NO llama al API ni hace el procesamiento pesado: lee el archivo que dejó
data_pipeline.py (data/ventas_actualizadas.parquet) y muestra todo.

Correr local:   streamlit run app.py
"""

import os
import json
import datetime as dt

import pandas as pd
import streamlit as st

import data_pipeline as dp


# ---------------------------------------------------------------------------
# Configuración de la página
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Panel de Ventas · Gaven", layout="wide")

PARQUET_PATH = dp.PARQUET_PATH
META_PATH = dp.META_PATH


# ---------------------------------------------------------------------------
# Helpers de formato
# ---------------------------------------------------------------------------

def fmt_money(x):
    try:
        return f"$ {x:,.0f}".replace(",", ".")
    except (TypeError, ValueError):
        return x


def fmt_kg(x):
    try:
        return f"{x:,.0f} kg".replace(",", ".")
    except (TypeError, ValueError):
        return x


# ---------------------------------------------------------------------------
# Lectura de datos locales (se cachea la LECTURA del archivo, no el API).
# La clave de caché incluye el mtime: si el pipeline reescribe el parquet,
# el mtime cambia y la caché se invalida sola.
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner="Leyendo datos...")
def cargar_datos_local(_mtime):
    return pd.read_parquet(PARQUET_PATH)


def leer_metadata():
    try:
        with open(META_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


# ---------------------------------------------------------------------------
# Rango de fechas: solo "Este Mes" o "Mes Anterior" (meses calendario, 2026)
# ---------------------------------------------------------------------------

def rango_mes(opcion, hoy=None):
    hoy = hoy or dt.date.today()
    if opcion == "Este Mes":
        desde = hoy.replace(day=1)          # primer día del mes actual
        hasta = hoy                         # hasta hoy
    else:  # "Mes Anterior"
        primer_dia_actual = hoy.replace(day=1)
        ultimo_dia_anterior = primer_dia_actual - dt.timedelta(days=1)  # último día mes anterior
        desde = ultimo_dia_anterior.replace(day=1)                      # primer día mes anterior
        hasta = ultimo_dia_anterior                                     # último día mes anterior
    return desde, hasta


# ---------------------------------------------------------------------------
# Carga + guardas
# ---------------------------------------------------------------------------

st.title("Panel de Ventas · Gaven")

if not os.path.exists(PARQUET_PATH):
    st.warning(
        "Todavía no hay datos cargados.\n\n"
        "Ejecutá primero el pipeline para generar el archivo:\n\n"
        "```\npython data_pipeline.py\n```"
    )
    st.stop()

df = cargar_datos_local(os.path.getmtime(PARQUET_PATH))

# Encabezado: selector de período integrado debajo del título (no en la sidebar)
col_sel, col_info = st.columns([1, 2])
with col_sel:
    opcion = st.radio(
        "Período",
        ["Este Mes", "Mes Anterior"],
        index=0,
        horizontal=True,
        label_visibility="collapsed",
    )

desde, hasta = rango_mes(opcion)

with col_info:
    meta = leer_metadata()
    ultima = meta.get("ultima_actualizacion", "—")
    st.caption(
        f"Período: {desde:%d/%m/%Y} → {hasta:%d/%m/%Y}  ·  "
        f"Última actualización de datos: {ultima}"
    )

st.divider()

# Filtro por fechas reales (no strings) + solo año 2026
fecha = df["fechaComprobate"]
df = df[
    (fecha >= pd.Timestamp(desde))
    & (fecha < pd.Timestamp(hasta) + pd.Timedelta(days=1))
    & (fecha.dt.year == 2026)
].copy()

if df.empty:
    st.warning("No hay datos para el período seleccionado.")
    st.stop()


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_general, tab_fs, tab_clientes = st.tabs(["General", "Food Service", "Clientes (RFM)"])


# --- TAB GENERAL ----------------------------------------------------------
with tab_general:
    m = dp.metricas_generales(df)

    c1, c2, c3 = st.columns(3)
    c1.metric("Kilos vendidos", fmt_kg(m["total_kilos"]))
    c2.metric("Subtotal Neto", fmt_money(m["subtotal_neto"]))
    c3.metric("Costo Total", fmt_money(m["costo_total"]))

    c4, c5, c6 = st.columns(3)
    c4.metric("Contribución Marginal", fmt_money(m["contribucion_marginal"]))
    c5.metric("CM %", f"{m['cm_pct']:.1f} %")
    c6.metric("Precio medio / kg", fmt_money(m["precio_medio_kg"]))

    st.divider()

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Kilos por región")
        st.bar_chart(dp.kilos_por_region(df).set_index("region")["kilos"])
    with col_b:
        st.subheader("Kilos por empresa")
        st.bar_chart(dp.kilos_por_empresa(df).set_index("dsEmpresa")["kilos"])

    st.subheader("Subtotal Neto por comprobante")
    st.dataframe(
        dp.subtotal_por_comprobante(df).style.format({"subtotalNeto": fmt_money}),
        use_container_width=True, hide_index=True,
    )


# --- TAB FOOD SERVICE -----------------------------------------------------
with tab_fs:
    fs, mfs = dp.food_service(df)

    if fs.empty:
        st.info("No hay ventas de Food Service en este período.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Kilos FS", fmt_kg(mfs["total_kilos"]))
        c2.metric("Subtotal Neto FS", fmt_money(mfs["subtotal_neto"]))
        c3.metric("Costo Total FS", fmt_money(mfs["costo_total"]))

        c4, c5 = st.columns(2)
        c4.metric("CM FS", fmt_money(mfs["contribucion_marginal"]))
        c5.metric("CM % FS", f"{mfs['cm_pct']:.1f} %")

        st.divider()
        st.subheader("Kilos de Food Service por región")
        st.bar_chart(dp.kilos_por_region(fs).set_index("region")["kilos"])


# --- TAB CLIENTES (RFM) ---------------------------------------------------
with tab_clientes:
    r = dp.rfm(df)

    if r.empty:
        st.info("No hay datos suficientes para el RFM.")
    else:
        st.subheader("Clientes que más compran (en $)")
        st.dataframe(
            r.sort_values("monetario", ascending=False).head(10)
            [["nombreCliente", "monetario", "frecuencia", "recencia"]]
            .style.format({"monetario": fmt_money}),
            use_container_width=True, hide_index=True,
        )

        st.subheader("Clientes que más veces compraron")
        st.dataframe(
            r.sort_values("frecuencia", ascending=False).head(10)
            [["nombreCliente", "frecuencia", "monetario", "recencia"]]
            .style.format({"monetario": fmt_money}),
            use_container_width=True, hide_index=True,
        )
