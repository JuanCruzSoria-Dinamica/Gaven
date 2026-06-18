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
# Formatos de tablas reutilizables
# ---------------------------------------------------------------------------

def fmt_pct(x):
    try:
        return f"{x:.1f} %"
    except (TypeError, ValueError):
        return x


# Columnas "estándar" que devuelve dp.agrupar_dim, con sus nombres lindos
COLS_DIM = {
    "kilos": "Kilos", "subtotalNeto": "Facturación", "cm": "Contribución",
    "cm_pct": "CM %", "precio_kg": "$/kg", "clientes": "Clientes",
    "share_fc": "Share FC %", "share_kg": "Share Kg %",
}
FMT_DIM = {
    "Kilos": fmt_kg, "Facturación": fmt_money, "Contribución": fmt_money,
    "CM %": fmt_pct, "$/kg": fmt_money, "Share FC %": fmt_pct, "Share Kg %": fmt_pct,
}


def tabla_dim(g, dim_label, dim_col):
    """Renderiza un resumen de dp.agrupar_dim como tabla formateada."""
    cols = [dim_col, "kilos", "subtotalNeto", "share_fc", "cm", "cm_pct",
            "precio_kg", "clientes"]
    cols = [c for c in cols if c in g.columns]
    t = g[cols].rename(columns={dim_col: dim_label, **COLS_DIM})
    st.dataframe(
        t.style.format(FMT_DIM), use_container_width=True, hide_index=True,
    )


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

(tab_resumen, tab_canales, tab_prod, tab_clientes,
 tab_vend, tab_alertas) = st.tabs(
    ["Resumen", "Canales", "Productos (SKU)", "Clientes (RFM)",
     "Vendedores", "Alertas"]
)


# --- TAB RESUMEN ----------------------------------------------------------
with tab_resumen:
    m = dp.metricas_generales(df)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Facturación neta", fmt_money(m["subtotal_neto"]))
    c2.metric("Kilos vendidos", fmt_kg(m["total_kilos"]))
    c3.metric("Contribución marginal", fmt_money(m["contribucion_marginal"]))
    c4.metric("CM %", fmt_pct(m["cm_pct"]))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Precio medio / kg", fmt_money(m["precio_medio_kg"]))
    c6.metric("Clientes únicos", f"{m['n_clientes']:,}".replace(",", "."))
    c7.metric("Ticket promedio", fmt_money(m["ticket_promedio"]))
    c8.metric("SKUs vendidos", f"{m['n_skus']:,}".replace(",", "."))

    st.caption(
        f"{m['n_comprobantes']:,}".replace(",", ".") + " comprobantes  ·  "
        + fmt_kg(m["kg_por_cliente"]) + " por cliente (promedio)"
    )

    st.divider()

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Facturación por canal")
        st.bar_chart(dp.por_canal(df).set_index("dsCanalMkt")["subtotalNeto"])
    with col_b:
        st.subheader("Kilos por región")
        st.bar_chart(dp.kilos_por_region(df).set_index("region")["kilos"])

    st.subheader("Kilos por empresa")
    st.bar_chart(dp.kilos_por_empresa(df).set_index("dsEmpresa")["kilos"])


# --- TAB CANALES ----------------------------------------------------------
with tab_canales:
    st.subheader("Detalle por canal")
    tabla_dim(dp.por_canal(df), "Canal", "dsCanalMkt")

    col_a, col_b = st.columns(2)
    with col_a:
        st.caption("Share de facturación por canal")
        st.bar_chart(dp.por_canal(df).set_index("dsCanalMkt")["share_fc"])
    with col_b:
        st.caption("CM % por canal")
        st.bar_chart(dp.por_canal(df).set_index("dsCanalMkt")["cm_pct"])

    st.divider()
    st.subheader("Detalle por subcanal")
    tabla_dim(dp.por_subcanal(df), "Subcanal", "dsSubcanalMKT")

    st.subheader("Detalle por marca / línea (proveedor)")
    tabla_dim(dp.por_proveedor(df), "Marca / Línea", "proveedor")


# --- TAB PRODUCTOS (SKU) --------------------------------------------------
with tab_prod:
    prod = dp.ranking_productos(df)

    if prod.empty:
        st.info("No hay productos en el período seleccionado.")
    else:
        n_a = int((prod["ABC"] == "A").sum())
        n_b = int((prod["ABC"] == "B").sum())
        n_c = int((prod["ABC"] == "C").sum())
        c1, c2, c3 = st.columns(3)
        c1.metric("SKUs clase A (80% FC)", n_a)
        c2.metric("SKUs clase B (80-95%)", n_b)
        c3.metric("SKUs clase C (resto)", n_c)

        st.divider()
        st.subheader("Ranking de productos con clasificación ABC")
        cols = ["dsArticulo", "ABC", "kilos", "subtotalNeto", "share_fc",
                "cm", "cm_pct", "precio_kg"]
        t = prod[cols].rename(columns={
            "dsArticulo": "Producto", **COLS_DIM,
        })
        st.dataframe(
            t.style.format(FMT_DIM), use_container_width=True, hide_index=True,
        )


# --- TAB CLIENTES (RFM) ---------------------------------------------------
with tab_clientes:
    r = dp.rfm(df)

    if r.empty:
        st.info("No hay datos suficientes para el RFM.")
    else:
        st.subheader("Segmentos de clientes (RFM)")
        seg = dp.resumen_segmentos(r)
        st.dataframe(
            seg.rename(columns={
                "segmento": "Segmento", "clientes": "Clientes",
                "facturacion": "Facturación",
            }).style.format({"Facturación": fmt_money}),
            use_container_width=True, hide_index=True,
        )

        st.divider()
        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Top clientes por facturación")
            st.dataframe(
                r.sort_values("monetario", ascending=False).head(10)
                [["nombreCliente", "segmento", "monetario", "frecuencia", "recencia"]]
                .rename(columns={
                    "nombreCliente": "Cliente", "segmento": "Segmento",
                    "monetario": "Facturación", "frecuencia": "Frecuencia",
                    "recencia": "Recencia (días)",
                })
                .style.format({"Facturación": fmt_money}),
                use_container_width=True, hide_index=True,
            )
        with col_b:
            st.subheader("Top clientes por frecuencia")
            st.dataframe(
                r.sort_values("frecuencia", ascending=False).head(10)
                [["nombreCliente", "segmento", "frecuencia", "monetario", "recencia"]]
                .rename(columns={
                    "nombreCliente": "Cliente", "segmento": "Segmento",
                    "frecuencia": "Frecuencia", "monetario": "Facturación",
                    "recencia": "Recencia (días)",
                })
                .style.format({"Facturación": fmt_money}),
                use_container_width=True, hide_index=True,
            )


# --- TAB VENDEDORES -------------------------------------------------------
with tab_vend:
    st.subheader("Detalle por vendedor")
    tabla_dim(dp.por_vendedor(df), "Vendedor", "dsVendedor")

    st.caption("Facturación por vendedor")
    st.bar_chart(dp.por_vendedor(df).set_index("dsVendedor")["subtotalNeto"])


# --- TAB ALERTAS ----------------------------------------------------------
with tab_alertas:
    st.subheader("Alertas e insights automáticos")
    avisos = dp.alertas(df)
    if not avisos:
        st.success("Sin alertas para el período seleccionado.")
    else:
        for a in avisos:
            if a["nivel"] == "riesgo":
                st.error(a["texto"])
            else:
                st.info(a["texto"])
