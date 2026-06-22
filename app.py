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
import calendar
import datetime as dt

import pandas as pd
import streamlit as st

import data_pipeline as dp


# ---------------------------------------------------------------------------
# Configuración de la página
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Panel de Ventas · Gaven",
    layout="wide",
)

# --- Estilos (paleta del tablero de referencia) ----------------------------
st.markdown(
    """
    <style>
      :root{
        --verde:#00b87a; --azul:#2a8ed4; --naranja:#f59e0b;
        --violeta:#a78bfa; --rojo:#f87171;
        --sf:#111827; --sf2:#1a2332; --border:#2a3a50; --tx2:#94a3b8;
      }
      .block-container{padding-top:2.2rem; max-width:1500px;}
      h1{font-weight:700; letter-spacing:-.5px;}
      /* Tarjetas de métricas */
      [data-testid="stMetric"]{
        background:var(--sf); border:1px solid var(--border);
        border-radius:12px; padding:14px 16px;
      }
      [data-testid="stMetricLabel"]{color:var(--tx2); font-size:.78rem;}
      [data-testid="stMetricValue"]{font-weight:700;}
      /* Tabs */
      .stTabs [data-baseweb="tab-list"]{gap:4px; border-bottom:1px solid var(--border);}
      .stTabs [data-baseweb="tab"]{
        border-radius:8px 8px 0 0; padding:8px 16px; font-weight:500;
      }
      .stTabs [aria-selected="true"]{
        background:var(--verde); color:#04221a !important;
      }
      /* Sidebar vacía: la ocultamos (los filtros van arriba) */
      section[data-testid="stSidebar"]{display:none;}
      /* Barra de filtros (contenedor con borde) */
      [data-testid="stVerticalBlockBorderWrapper"]{
        background:var(--sf); border-radius:12px;
      }
      /* Subtítulos */
      h3{color:#cbd5e1; font-weight:600; letter-spacing:-.2px;}
    </style>
    """,
    unsafe_allow_html=True,
)

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


# ---------------------------------------------------------------------------
# Barra de filtros (arriba). Todos los filtros son selectores y aplican a
# TODAS las solapas (filtro global), igual que el tablero de referencia.
# ---------------------------------------------------------------------------

def opciones(serie):
    """Lista ordenada de valores únicos no vacíos para un multiselect."""
    vals = (
        serie.dropna().astype(str).str.strip()
        .replace({"": None, "0": None}).dropna().unique().tolist()
    )
    return sorted(vals)


# Cada filtro es una tupla: (etiqueta, columna). Solo se muestran los que
# realmente tienen datos en el período.
FILTROS = [
    ("Canal", "dsCanalMkt"),
    ("Subcanal", "dsSubcanalMKT"),
    ("Región", "region"),
    ("Vendedor", "dsVendedor"),
    ("Marca / Línea", "proveedor"),
    ("Cliente", "nombreCliente"),
]

with st.container(border=True):
    # Fila 1: período + Top N + última actualización
    f1a, f1b, f1c = st.columns([1.6, 1.2, 1.6])
    opcion = f1a.radio(
        "Período", ["Este Mes", "Mes Anterior"], index=0, horizontal=True
    )
    top_n = f1b.select_slider(
        "Top N (rankings)", options=[5, 10, 15, 25, 50], value=10
    )
    desde, hasta = rango_mes(opcion)

    meta = leer_metadata()
    ultima = meta.get("ultima_actualizacion", "—")
    f1c.markdown(
        f"<div style='text-align:right;color:var(--tx2);font-size:.8rem;"
        f"padding-top:1.9rem'>Última actualización: {ultima}</div>",
        unsafe_allow_html=True,
    )

    # df del período (base para construir las opciones de los selectores)
    fecha = df["fechaComprobate"]
    df_periodo = df[
        (fecha >= pd.Timestamp(desde))
        & (fecha < pd.Timestamp(hasta) + pd.Timedelta(days=1))
        & (fecha.dt.year == 2026)
    ].copy()

    # Fila 2: un selector por dimensión (solo las que tienen datos)
    seleccion = {}
    if not df_periodo.empty:
        disponibles = [
            (et, col) for et, col in FILTROS
            if col in df_periodo.columns and opciones(df_periodo[col])
        ]
        if disponibles:
            cols = st.columns(len(disponibles))
            for i, (etiqueta, col) in enumerate(disponibles):
                seleccion[col] = cols[i].multiselect(
                    etiqueta, opciones(df_periodo[col]), default=[],
                    placeholder="Todos",
                )

n_filtros = sum(1 for v in seleccion.values() if v)
chip = f"  ·  {n_filtros} filtro(s) activo(s)" if n_filtros else "  ·  sin filtros"
st.caption(f"Período: {desde:%d/%m/%Y} → {hasta:%d/%m/%Y}{chip}")
st.divider()

if df_periodo.empty:
    st.warning("No hay datos para el período seleccionado.")
    st.stop()

# Aplica los filtros seleccionados (los vacíos no filtran nada)
df = df_periodo
for col, valores in seleccion.items():
    if valores:
        df = df[df[col].astype(str).str.strip().isin(valores)]

if df.empty:
    st.warning("No hay datos que cumplan con los filtros seleccionados.")
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

    # --- Tendencia: proyección de cierre de mes a ritmo actual --------------
    # Solo aplica a "Este Mes" (el mes anterior ya está completo, factor = 1).
    # Fórmula: Venta acumulada / días transcurridos × días totales del mes.
    if opcion == "Este Mes":
        hoy = dt.date.today()
        dias_transcurridos = hoy.day                                   # día de hoy (calendario)
        dias_del_mes = calendar.monthrange(hoy.year, hoy.month)[1]     # total de días del mes
        factor = (dias_del_mes / dias_transcurridos) if dias_transcurridos else 1

        st.divider()
        st.subheader("Tendencia a fin de mes (proyección)")
        st.caption(
            f"Proyección a ritmo actual · {dias_transcurridos} de {dias_del_mes} "
            f"días transcurridos (factor ×{factor:.2f})."
        )
        t1, t2, t3 = st.columns(3)
        t1.metric("Facturación proyectada", fmt_money(m["subtotal_neto"] * factor))
        t2.metric("Kilos proyectados", fmt_kg(m["total_kilos"] * factor))
        t3.metric("Contribución proyectada", fmt_money(m["contribucion_marginal"] * factor))

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
        st.subheader(f"Ranking de productos con clasificación ABC · Top {top_n}")
        cols = ["dsArticulo", "ABC", "kilos", "subtotalNeto", "share_fc",
                "cm", "cm_pct", "precio_kg"]
        t = prod[cols].head(top_n).rename(columns={
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
                r.sort_values("monetario", ascending=False).head(top_n)
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
                r.sort_values("frecuencia", ascending=False).head(top_n)
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
