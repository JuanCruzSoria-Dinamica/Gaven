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
import plotly.express as px

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


@st.cache_data(show_spinner="Leyendo serie histórica...")
def cargar_serie(_mtime):
    """Lee la serie mensual agregada (data/serie_mensual.parquet).
    La clave de caché es el mtime: si el pipeline reescribe la serie, se
    invalida sola (mismo patrón que cargar_datos_local)."""
    return pd.read_parquet(dp.SERIE_PATH)


@st.cache_data(show_spinner="Leyendo IPC (INDEC)...")
def cargar_ipc(_mtime=None):
    """Devuelve el IPC del INDEC. Usa el archivo que deja el pipeline; si todavía
    no existe (ej. antes de la primera corrida del cron), intenta bajarlo una vez.
    La clave de caché es el mtime del archivo: cuando el pipeline reescribe el
    IPC, la caché se invalida sola (igual que la serie). Así nunca queda
    'pegado' un IPC vacío."""
    ipc = dp.cargar_ipc()
    if ipc.empty:
        try:
            ipc = dp.descargar_ipc()
        except Exception:
            pass
    return ipc


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
    # Fila 1: período + última actualización
    f1a, f1c = st.columns([2, 2])
    opcion = f1a.radio(
        "Período", ["Este Mes", "Mes Anterior"], index=0, horizontal=True
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

    # Fila 2: un selector por dimensión (solo las que tienen datos).
    # Filtros EN CASCADA: las opciones de cada selector se calculan sobre el
    # df ya filtrado por los OTROS selectores. Así, si filtrás por "food
    # service", el selector de Vendedor solo ofrece los que vendieron eso.
    seleccion = {}
    if not df_periodo.empty:
        disponibles = [
            (et, col) for et, col in FILTROS
            if col in df_periodo.columns and opciones(df_periodo[col])
        ]
        if disponibles:
            # Selecciones de la corrida anterior (Streamlit re-ejecuta en cada
            # interacción): sirven de base para armar las opciones cruzadas.
            sel_prev = {
                col: st.session_state.get(f"filtro_{col}", [])
                for _, col in disponibles
            }

            def _df_filtrado_excepto(col_excluida):
                """df del período filtrado por todos los selectores menos uno."""
                d = df_periodo
                for c, vals in sel_prev.items():
                    if c == col_excluida or not vals:
                        continue
                    d = d[d[c].astype(str).str.strip().isin(vals)]
                return d

            cols = st.columns(len(disponibles))
            for i, (etiqueta, col) in enumerate(disponibles):
                opts = opciones(_df_filtrado_excepto(col)[col])
                key = f"filtro_{col}"
                # Si algún valor elegido ya no es válido (porque otro filtro lo
                # excluyó), lo sacamos del estado para evitar el error de
                # Streamlit "default value not in options".
                if key in st.session_state:
                    st.session_state[key] = [
                        v for v in st.session_state[key] if v in opts
                    ]
                seleccion[col] = cols[i].multiselect(
                    etiqueta, opts, key=key, placeholder="Todos",
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

    # --- Proyección a fin de mes ------------------------------------------
    # Run-rate lineal: extrapola lo acumulado hasta hoy al total del mes,
    # usando el ritmo diario promedio. Cuenta solo días HÁBILES: los domingos
    # no se trabaja, así que no entran ni en los transcurridos ni en el total.
    # Solo aplica al "Este Mes" en curso; "Mes Anterior" ya está cerrado.
    def _dias_habiles(anio, mes, hasta_dia):
        """Días no-domingo del 1 al hasta_dia (inclusive) de un mes."""
        return sum(
            1 for d in range(1, hasta_dia + 1)
            if dt.date(anio, mes, d).weekday() != 6  # 6 = domingo
        )

    factor = 1.0
    proyectar = False
    if opcion == "Este Mes":
        total_dias = calendar.monthrange(hasta.year, hasta.month)[1]
        ult = df["fechaComprobate"].max()
        dia_ult = ult.day if pd.notna(ult) else hasta.day
        hab_mes = _dias_habiles(hasta.year, hasta.month, total_dias)
        hab_trans = _dias_habiles(hasta.year, hasta.month, dia_ult)
        if hab_trans and hab_trans < hab_mes:
            factor = hab_mes / hab_trans
            proyectar = True

    def proy(col, valor, fmt, escala=True):
        """Muestra debajo de la métrica la proyección a fin de mes.

        escala=True  -> métrica aditiva (se multiplica por el run-rate).
        escala=False -> métrica de tasa/ratio (se mantiene estable).
        """
        if not proyectar:
            return
        pv = valor * factor if escala else valor
        col.caption(f"Proy. fin de mes: {fmt(pv)}")

    def _int(x):
        return f"{round(x):,}".replace(",", ".")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Facturación neta", fmt_money(m["subtotal_neto"]))
    proy(c1, m["subtotal_neto"], fmt_money)
    c2.metric("Kilos vendidos", fmt_kg(m["total_kilos"]))
    proy(c2, m["total_kilos"], fmt_kg)
    c3.metric("Contribución marginal", fmt_money(m["contribucion_marginal"]))
    proy(c3, m["contribucion_marginal"], fmt_money)
    c4.metric("CM %", fmt_pct(m["cm_pct"]))
    proy(c4, m["cm_pct"], fmt_pct, escala=False)

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Precio medio / kg", fmt_money(m["precio_medio_kg"]))
    proy(c5, m["precio_medio_kg"], fmt_money, escala=False)
    c6.metric("Clientes únicos", f"{m['n_clientes']:,}".replace(",", "."))
    proy(c6, m["n_clientes"], _int)
    c7.metric("Ticket promedio", fmt_money(m["ticket_promedio"]))
    proy(c7, m["ticket_promedio"], fmt_money, escala=False)
    c8.metric("SKUs vendidos", f"{m['n_skus']:,}".replace(",", "."))
    proy(c8, m["n_skus"], _int)

    st.caption(
        f"{m['n_comprobantes']:,}".replace(",", ".") + " comprobantes  ·  "
        + fmt_kg(m["kg_por_cliente"]) + " por cliente (promedio)"
    )

    st.divider()

    # --- Evolución mensual por canal --------------------------------------
    # Usa la serie mensual histórica (data/serie_mensual.parquet),
    # INDEPENDIENTE del filtro de período: muestra todos los meses 2025–2026
    # para comparar.
    st.subheader("Evolución mensual por canal")

    if not os.path.exists(dp.SERIE_PATH):
        st.warning(
            "Todavía no existe la serie histórica.\n\n"
            "Generala UNA vez con el backfill:\n\n"
            "```\npython backfill_serie.py\n```\n\n"
            "Después el pipeline normal la mantiene actualizada sola."
        )
    else:
        serie = cargar_serie(os.path.getmtime(dp.SERIE_PATH))

        if serie.empty:
            st.info("La serie histórica está vacía.")
        else:
            METRICAS_EVOL = {
                "Facturación neta": ("subtotalNeto", fmt_money),
                "Kilos": ("kilos", fmt_kg),
                "Contribución marginal (MB $)": ("cm", fmt_money),
                "CM % (margen)": ("cm_pct", fmt_pct),
                "Precio medio $/kg": ("precio_kg", fmt_money),
            }

            c1, c2, c3 = st.columns([1.5, 0.9, 1.2])
            nombre_metrica = c1.selectbox(
                "Métrica", list(METRICAS_EVOL.keys()), index=0
            )
            nivel = c2.radio(
                "Abrir por", ["Canal", "Subcanal"], horizontal=True
            )
            moneda = c3.radio(
                "Moneda", ["Corriente", "Constante (s/ inflación)"],
                horizontal=True,
                help="Corriente = pesos de cada mes (nominal). "
                     "Constante = todo llevado a pesos de hoy con el IPC del "
                     "INDEC, para comparar sin el efecto de la inflación.",
            )
            dim = "dsCanalMkt" if nivel == "Canal" else "dsSubcanalMKT"

            # Respeta los filtros globales de canal/subcanal si están activos.
            s = serie.copy()
            if seleccion.get("dsCanalMkt"):
                s = s[s["dsCanalMkt"].astype(str).str.strip()
                      .isin(seleccion["dsCanalMkt"])]
            if seleccion.get("dsSubcanalMKT"):
                s = s[s["dsSubcanalMKT"].astype(str).str.strip()
                      .isin(seleccion["dsSubcanalMKT"])]

            # Re-agrega al nivel elegido (las sumas crudas se re-agregan bien).
            g = (
                s.groupby(["anio_mes", dim], dropna=False)
                .agg(
                    kilos=("kilos", "sum"),
                    subtotalNeto=("subtotalNeto", "sum"),
                    cm=("cm", "sum"),
                )
                .reset_index()
            )

            # --- Pesos constantes: deflactar $ con el IPC del INDEC ---------
            base_mes = None
            nota_moneda = "Pesos corrientes (nominales, de cada mes)."
            if moneda.startswith("Constante"):
                _ipc_mtime = (os.path.getmtime(dp.IPC_PATH)
                              if os.path.exists(dp.IPC_PATH) else None)
                ipc = cargar_ipc(_ipc_mtime)
                factores, base_mes = dp.factores_constantes(ipc)
                if not factores:
                    st.warning(
                        "No hay IPC disponible todavía (corré el pipeline o "
                        "esperá a que INDEC responda). Mostrando pesos corrientes."
                    )
                else:
                    # Factor por mes; meses sin IPC (ej. mes en curso) usan el
                    # último factor disponible (≈1 respecto del mes base).
                    ult = min(factores.values())  # el del mes más reciente
                    fac = g["anio_mes"].map(factores).fillna(ult)
                    g["subtotalNeto"] = g["subtotalNeto"] * fac
                    g["cm"] = g["cm"] * fac
                    nota_moneda = (
                        f"Pesos constantes de {base_mes} (deflactado con IPC "
                        f"Nivel General INDEC). Kilos y % no se ven afectados."
                    )

            # Métricas derivadas (porcentaje y $/kg se calculan acá, no se guardan).
            den_fc = g["subtotalNeto"].replace(0, pd.NA)
            den_kg = g["kilos"].replace(0, pd.NA)
            g["cm_pct"] = (g["cm"] / den_fc * 100).fillna(0)
            g["precio_kg"] = (g["subtotalNeto"] / den_kg).fillna(0)

            col_val, _fmt = METRICAS_EVOL[nombre_metrica]
            g = g.sort_values(["anio_mes", dim])

            # --- Total por mes (suma de todos los canales/subcanales) --------
            # Las métricas aditivas se suman; los % y $/kg se recalculan sobre
            # los totales para que el "Total" sea correcto (no un promedio).
            tot = (
                g.groupby("anio_mes", as_index=False)
                .agg(kilos=("kilos", "sum"),
                     subtotalNeto=("subtotalNeto", "sum"),
                     cm=("cm", "sum"))
            )
            tot_den_fc = tot["subtotalNeto"].replace(0, pd.NA)
            tot_den_kg = tot["kilos"].replace(0, pd.NA)
            tot["cm_pct"] = (tot["cm"] / tot_den_fc * 100).fillna(0)
            tot["precio_kg"] = (tot["subtotalNeto"] / tot_den_kg).fillna(0)
            tot = tot.sort_values("anio_mes")

            fig = px.line(
                g, x="anio_mes", y=col_val, color=dim, markers=True,
            )
            fig.add_scatter(
                x=tot["anio_mes"], y=tot[col_val], mode="lines+markers",
                name="Total", line=dict(color="#e5e7eb", width=3, dash="dash"),
                marker=dict(size=6),
            )
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=10, b=10),
                legend=dict(title=nivel, orientation="h", y=-0.2),
                xaxis_title="Mes",
                yaxis_title=nombre_metrica,
                height=440,
            )
            st.plotly_chart(fig, use_container_width=True)
            st.caption(
                f"{nota_moneda}  ·  Serie completa (no depende del filtro de "
                "período de arriba). El mes en curso puede estar incompleto."
            )

            # Tabla pivote opcional (meses en columnas) para ver los números.
            with st.expander("Ver tabla de valores"):
                piv = g.pivot_table(
                    index=dim, columns="anio_mes", values=col_val,
                    aggfunc="sum",
                )
                st.dataframe(
                    piv.style.format(_fmt), use_container_width=True
                )


# --- TAB CANALES ----------------------------------------------------------
with tab_canales:
    st.subheader("Detalle por canal")
    tabla_dim(dp.por_canal(df), "Canal", "dsCanalMkt")

    col_a, col_b = st.columns(2)
    with col_a:
        st.caption("Share de facturación por canal")
        _pc = dp.por_canal(df)
        fig_torta = px.pie(
            _pc, names="dsCanalMkt", values="share_fc", hole=0.4,
        )
        fig_torta.update_traces(textposition="inside", textinfo="percent+label")
        fig_torta.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False,
            height=360,
        )
        st.plotly_chart(fig_torta, use_container_width=True)
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
        c_titulo, c_top = st.columns([3, 1])
        top_n = c_top.select_slider(
            "Top N", options=[5, 10, 15, 25, 50], value=10, key="top_n_prod"
        )
        c_titulo.subheader(
            f"Ranking de productos con clasificación ABC · Top {top_n}"
        )
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
        # Filtro por segmento: si elegís "Campeones" (o varios), las tablas de
        # abajo muestran el top SOLO de ese/esos segmento(s). Vacío = todos.
        ORDEN_SEG = ["Campeones", "Leales", "Nuevos / Prometedores",
                     "En riesgo", "Hibernando / Perdidos"]
        segs_disp = [s for s in ORDEN_SEG if s in set(r["segmento"])]
        c_seg, c_top = st.columns([3, 1])
        seg_sel = c_seg.multiselect(
            "Segmento", segs_disp, default=[],
            placeholder="Todos los segmentos", key="seg_rfm",
        )
        top_n = c_top.select_slider(
            "Top N", options=[5, 10, 15, 25, 50], value=10, key="top_n_rfm"
        )
        r_f = r[r["segmento"].isin(seg_sel)] if seg_sel else r
        if r_f.empty:
            st.info("No hay clientes en el segmento seleccionado.")
        else:
            col_a, col_b = st.columns(2)
            with col_a:
                st.subheader("Top clientes por facturación")
                st.dataframe(
                    r_f.sort_values("monetario", ascending=False).head(top_n)
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
                    r_f.sort_values("frecuencia", ascending=False).head(top_n)
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
