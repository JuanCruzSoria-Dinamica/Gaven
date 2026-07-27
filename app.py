"""
app.py
------
Panel de ventas (Gaven). SOLO presentación (todos los meses de 2026).

NO llama al API ni hace el procesamiento pesado: lee el archivo que dejó
data_pipeline.py (data/ventas_actualizadas.parquet) y muestra todo.

Correr local:   streamlit run app.py
"""

import io
import os
import json
import time
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
      /* FIX: mantener ocultas las solapas inactivas.
         Streamlit manda el contenido de TODAS las solapas al navegador y solo
         esconde las inactivas con CSS. Cuando un widget dentro de una solapa
         (ej. el evolutivo) dispara un rerun, esa regla de ocultamiento a veces
         se pierde y todo el contenido aparece apilado en todas las solapas.
         Forzamos que los paneles inactivos ([hidden]) sigan ocultos. */
      .stTabs [data-baseweb="tab-panel"][hidden],
      .stTabs [role="tabpanel"][hidden]{
        display:none !important;
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
    df = pd.read_parquet(PARQUET_PATH)
    # 'marca_linea' es una columna DERIVADA del lookup por código
    # (data/proveedor_objetivo_lookup.csv). Se recalcula siempre al leer para
    # que la clasificación refleje el lookup vigente aunque el parquet guardado
    # traiga valores viejos. Es barato (map por idArticulo).
    df = dp.agregar_marca_linea(df)
    return df


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
# Rango de fechas: cualquier mes de 2026 con datos en el parquet.
# El pipeline mantiene el detalle de TODO el año por upsert mensual, así que
# acá solo listamos los meses disponibles y armamos el rango del elegido.
# ---------------------------------------------------------------------------

MESES_ES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
            "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]


def rango_mes(anio_mes, hoy=None):
    """(desde, hasta) del mes calendario 'YYYY-MM'. El mes en curso se corta
    en hoy; los meses cerrados van del día 1 al último día del mes."""
    hoy = hoy or dt.date.today()
    anio, mes = map(int, str(anio_mes).split("-"))
    desde = dt.date(anio, mes, 1)
    ultimo = dt.date(anio, mes, calendar.monthrange(anio, mes)[1])
    return desde, min(ultimo, hoy)


def etiqueta_mes(anio_mes, hoy=None):
    """'2026-07' -> 'Julio 2026 (Actual)' / '2026-03' -> 'Marzo 2026'."""
    hoy = hoy or dt.date.today()
    anio, mes = map(int, str(anio_mes).split("-"))
    lbl = f"{MESES_ES[mes - 1]} {anio}"
    if anio_mes == hoy.strftime("%Y-%m"):
        lbl += " (Actual)"
    return lbl


def meses_disponibles(df, anio=None):
    """Meses 'YYYY-MM' del año con datos en el parquet, más reciente primero."""
    anio = anio or dp.ANIO
    f = pd.to_datetime(df["fechaComprobate"], errors="coerce").dropna()
    f = f[f.dt.year == anio]
    return sorted(f.dt.strftime("%Y-%m").unique(), reverse=True)


# ---------------------------------------------------------------------------
# Login + roles
# ---------------------------------------------------------------------------
# Dos roles, sin base de datos de usuarios. Las credenciales viven en
# .streamlit/secrets.toml (sección [acceso]), NO en este archivo.
#   - "dueno"      -> ve todo, incluida Contribución marginal y CM %.
#   - "supervisor" -> ve todo MENOS Contribución marginal y CM %.
# El rol se guarda en st.session_state y sobrevive a las re-ejecuciones.

def _login():
    """Muestra el login y corta la ejecución hasta que el rol esté seteado."""
    if "rol" not in st.session_state:
        st.session_state.rol = None
    if st.session_state.rol is not None:
        # Pantalla de carga de 1 segundo, solo justo después de loguearse.
        if st.session_state.pop("_cargando_login", False):
            st.markdown(
                """
                <style>
                  @keyframes girar{to{transform:rotate(360deg);}}
                  /* Overlay a pantalla completa: tapa el contenido "viejo"
                     que Streamlit deja visible mientras corre el script. */
                  .carga-login{
                    position:fixed; inset:0; z-index:999999;
                    background:#0a0e17;
                    display:flex; flex-direction:column; align-items:center;
                    justify-content:center; gap:18px;
                  }
                  .carga-login .aro{
                    width:44px; height:44px; border-radius:50%;
                    border:4px solid var(--border);
                    border-top-color:var(--verde);
                    animation:girar .8s linear infinite;
                  }
                  .carga-login p{color:var(--tx2); font-size:.9rem; margin:0;}
                </style>
                <div class="carga-login">
                  <div class="aro"></div>
                  <p>Cargando panel…</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            time.sleep(1)
            st.rerun()
        return  # ya está logueado, seguimos con el tablero

    try:
        cred = st.secrets["acceso"]
    except Exception:
        st.error(
            "Falta la sección [acceso] en .streamlit/secrets.toml. "
            "Agregá usuarios y contraseñas para poder entrar."
        )
        st.stop()

    # Login centrado en una "cajita" angosta (como cualquier sitio web):
    # tres columnas y el formulario va en la del medio.
    st.markdown(
        """
        <style>
          /* Quitamos el borde propio del form: la "caja" la pone el
             contenedor con borde de afuera, así no se duplica. */
          [data-testid="stForm"]{border:0; padding:0;}
          .login-head{text-align:center; margin:0 0 16px;}
          .login-head h2{margin:0; font-weight:700; letter-spacing:-.3px;
            font-size:1.25rem;}
          .login-head p{color:var(--tx2); font-size:.85rem; margin:.25rem 0 0;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    _l, _c, _r = st.columns([1.4, 1, 1.4])
    with _c:
        st.markdown("<div style='height:6vh'></div>", unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown(
                "<div class='login-head'>"
                "<h2>Panel de Ventas · Gaven</h2>"
                "<p>Iniciá sesión para continuar</p>"
                "</div>",
                unsafe_allow_html=True,
            )
            with st.form("login"):
                usuario = st.text_input("Usuario")
                pwd = st.text_input("Contraseña", type="password")
                entrar = st.form_submit_button(
                    "Entrar", type="primary", use_container_width=True
                )
        if entrar:
            if (usuario == cred.get("usuario_duenos")
                    and pwd == cred.get("password_duenos")):
                st.session_state.rol = "dueno"
                st.session_state._cargando_login = True
                st.rerun()
            elif (usuario == cred.get("usuario_supervisores")
                    and pwd == cred.get("password_supervisores")):
                st.session_state.rol = "supervisor"
                st.session_state._cargando_login = True
                st.rerun()
            else:
                st.error("Usuario o contraseña incorrectos.")
    st.stop()  # mientras no haya rol válido, no se renderiza nada del tablero


_login()

# A partir de acá hay un rol válido en sesión.
# mostrar_cm es la llave maestra: si es False, los números de Contribución
# marginal y CM % no se calculan ni se muestran en NINGÚN lado del tablero.
mostrar_cm = st.session_state.rol == "dueno"


# ---------------------------------------------------------------------------
# Carga + guardas
# ---------------------------------------------------------------------------

# Encabezado con el rol activo y botón para cerrar sesión.
_ct, _cu = st.columns([4, 1])
_ct.title("Panel de Ventas · Gaven")
_rol_label = "Dueño" if st.session_state.rol == "dueno" else "Supervisor"
_cu.markdown(
    f"<div style='text-align:right;color:var(--tx2);font-size:.8rem;"
    f"padding-top:1rem'>Sesión: <b>{_rol_label}</b></div>",
    unsafe_allow_html=True,
)
if _cu.button("Cerrar sesión", use_container_width=True):
    st.session_state.rol = None
    st.rerun()

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
    ("Marca / Línea", "marca_linea"),
    ("Cliente", "nombreCliente"),
]

with st.container(border=True):
    # Fila 1: período + última actualización
    f1a, f1c = st.columns([2, 2])
    _meses_disp = meses_disponibles(df)
    if not _meses_disp:
        st.warning(
            f"El parquet no tiene datos de {dp.ANIO}. "
            "Corré el pipeline: `python data_pipeline.py`"
        )
        st.stop()
    mes_sel = f1a.selectbox(
        "Período", _meses_disp, index=0, format_func=etiqueta_mes,
        help="Todos los meses de 2026 con datos. El pipeline trae los meses "
             "faltantes una sola vez y después solo actualiza el mes en curso "
             "y el anterior.",
    )
    es_mes_actual = mes_sel == dt.date.today().strftime("%Y-%m")
    desde, hasta = rango_mes(mes_sel)

    # El mes en curso se corta en la última fecha CON DATOS, no en hoy: si el
    # pipeline corrió con --hasta (corte a un día específico), la etiqueta
    # "Período" refleja ese corte real y no la fecha de hoy.
    if es_mes_actual:
        _ult_dato = df["fechaComprobate"].max()
        if pd.notna(_ult_dato):
            hasta = min(hasta, _ult_dato.date())

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
        & (fecha.dt.year == dp.ANIO)
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
    "skus": "SKUs", "skus_por_cliente": "SKUs/Cliente",
    "share_fc": "Share FC %", "share_kg": "Share Kg %",
}
FMT_DIM = {
    "Kilos": fmt_kg, "Facturación": fmt_money, "Contribución": fmt_money,
    "CM %": fmt_pct, "$/kg": fmt_money, "Share FC %": fmt_pct, "Share Kg %": fmt_pct,
    "SKUs/Cliente": lambda x: f"{x:,.1f}".replace(",", "."),
}


def tabla_dim(g, dim_label, dim_col, mostrar_skus=False,
              mostrar_skus_cliente=False):
    """Renderiza un resumen de dp.agrupar_dim como tabla formateada.

    mostrar_skus=True agrega la columna 'SKUs' (cantidad de productos únicos
    que maneja cada fila de la dimensión).
    mostrar_skus_cliente=True agrega 'SKUs/Cliente' (productos únicos
    promedio por cliente)."""
    cols = [dim_col, "kilos", "subtotalNeto", "share_fc", "cm", "cm_pct",
            "precio_kg", "clientes"]
    if mostrar_skus:
        cols.append("skus")
    if mostrar_skus_cliente:
        cols.append("skus_por_cliente")
    # Supervisores no ven Contribución ni CM %: se quitan las columnas.
    if not mostrar_cm:
        cols = [c for c in cols if c not in ("cm", "cm_pct")]
    cols = [c for c in cols if c in g.columns]
    t = g[cols].rename(columns={dim_col: dim_label, **COLS_DIM})
    st.dataframe(
        t.style.format(FMT_DIM), use_container_width=True, hide_index=True,
    )
    # Se devuelve la tabla ya renombrada (y sin CM si el rol no la ve) para
    # poder incluirla en el Excel descargable de la solapa.
    return t


# ---------------------------------------------------------------------------
# Descarga en Excel (un botón por solapa)
# ---------------------------------------------------------------------------
# El botón nativo de las tablas de Streamlit solo baja CSV crudo. Estos
# helpers arman un .xlsx real (openpyxl) con una hoja por tabla, números como
# números y anchos de columna razonables. Las tablas que reciben ya vienen
# renombradas y filtradas por rol (sin CM para supervisores).

def _excel_bytes(hojas):
    """hojas: dict {nombre_hoja: DataFrame} -> bytes de un .xlsx."""
    from openpyxl.utils import get_column_letter

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for nombre, t in hojas.items():
            if t is None or len(t) == 0:
                continue
            nombre = str(nombre)[:31]  # límite de Excel
            t.to_excel(w, sheet_name=nombre, index=False)
            ws = w.sheets[nombre]
            for i, c in enumerate(t.columns, 1):
                letra = get_column_letter(i)
                # Fechas: formato corto dd/mm/aaaa (sin la hora 00:00:00,
                # que hace que Excel muestre '####' si la columna es angosta).
                if pd.api.types.is_datetime64_any_dtype(t[c]):
                    for celda in ws[letra][1:]:
                        celda.number_format = "DD/MM/YYYY"
                    ws.column_dimensions[letra].width = 14
                    continue
                ancho = max(
                    [len(str(c))]
                    + t[c].astype(str).str.len().head(200).tolist()
                )
                ws.column_dimensions[letra].width = (
                    min(max(ancho + 2, 10), 45)
                )
    return buf.getvalue()


def boton_excel(nombre, hojas, key):
    """Botón de descarga de un .xlsx con las tablas de la solapa."""
    hojas = {n: t for n, t in hojas.items() if t is not None and len(t)}
    if not hojas:
        return
    st.download_button(
        "Descargar Excel",
        data=_excel_bytes(hojas),
        file_name=f"{nombre}_{desde:%Y-%m-%d}_a_{hasta:%Y-%m-%d}.xlsx",
        mime=("application/vnd.openxmlformats-officedocument"
              ".spreadsheetml.sheet"),
        key=key,
    )


# ---------------------------------------------------------------------------
# Navegación progresiva (árbol de descomposición)
# ---------------------------------------------------------------------------
# Reemplaza los cruces sueltos de las solapas Proveedores y Productos (SKU):
# un único recorrido por niveles (ej. Línea → Canal → Vendedor → Cliente →
# SKU) donde cada nivel muestra SOLO lo compatible con lo ya elegido y la
# participación % se calcula dentro del nivel anterior.

def _cols_cm(cols):
    """Quita las columnas de CM si el rol no puede verlas."""
    return cols if mostrar_cm else [c for c in cols if c not in ("cm", "cm_pct")]


def _barras_share(g, col_dim, etiqueta, col_val, col_share, top_n=12):
    """Barras horizontales de composición: top N + 'OTRAS', con el share
    % como texto. Devuelve la figura lista para st.plotly_chart."""
    top = g.nlargest(top_n, col_val).copy()
    resto = g[~g[col_dim].isin(top[col_dim])]
    if len(resto):
        fila = {col_dim: f"OTRAS ({len(resto)})",
                col_val: resto[col_val].sum(),
                col_share: resto[col_share].sum()}
        top = pd.concat([top, pd.DataFrame([fila])], ignore_index=True)
    top = top.sort_values(col_val)
    fig = px.bar(
        top, x=col_val, y=col_dim, orientation="h",
        text=top[col_share].map(lambda v: f"{v:.1f} %"),
    )
    fig.update_traces(textposition="outside", cliponaxis=False,
                      marker_color="#00b87a")
    fig.update_layout(
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=60, t=10, b=10),
        xaxis_title=None, yaxis_title=None,
        height=max(300, 30 * len(top) + 60),
    )
    return fig


# Métrica principal elegible en el drill: (columna de valor, columna de share)
METRICAS_DRILL = {
    "Facturación": ("subtotalNeto", "share_fc"),
    "Kilos": ("kilos", "share_kg"),
    "Contribución": ("cm", "share_cm"),
}


def render_drill(df_base, niveles, key, root_id=None):
    """Navegación progresiva por niveles (tipo árbol de descomposición).

    df_base : datos ya filtrados (filtros globales y, si aplica, la raíz,
              ej. el producto elegido en el ranking).
    niveles : lista de (etiqueta, columna) en orden de navegación.
              El último nivel es solo informativo (no se puede seleccionar).
    key     : prefijo único para session_state (permite varios drills).
    root_id : identificador de la raíz. Si cambia (ej. se elige otro
              producto), el recorrido se reinicia solo.

    El recorrido vive en session_state como lista de valores elegidos.
    En cada corrida se valida contra los datos vigentes: si un filtro
    global dejó afuera un valor elegido, el camino se corta en el último
    nivel válido (evita pantallas vacías).

    Devuelve la tabla completa del nivel visible (renombrada y sin CM si
    el rol no la ve) para sumarla al Excel de la solapa, o None.
    """
    k_path, k_nonce, k_root = f"{key}_path", f"{key}_nonce", f"{key}_root"
    if k_path not in st.session_state:
        st.session_state[k_path] = []
    if k_nonce not in st.session_state:
        # El nonce entra en la key de la tabla clickeable de cada nivel y se
        # incrementa CADA vez que el recorrido cambia (avanzar, volver,
        # reiniciar). Así el widget nuevo nace sin selección "pegada" de una
        # visita anterior al mismo nivel (gotcha clásico de Streamlit).
        st.session_state[k_nonce] = 0

    def _set_path(nuevo):
        st.session_state[k_path] = nuevo
        st.session_state[k_nonce] += 1

    # Cambió la raíz (ej. otro producto en el ranking): arrancar de cero.
    if st.session_state.get(k_root, "__sin_raiz__") != root_id:
        st.session_state[k_root] = root_id
        _set_path([])

    if df_base.empty:
        st.info("No hay datos para navegar con los filtros actuales.")
        return None

    # --- Validación del recorrido contra los datos vigentes ---------------
    path = st.session_state[k_path]
    d = df_base
    validos = []
    for i, val in enumerate(path):
        if i >= len(niveles) - 1:
            break  # nunca se navega más allá del anteúltimo nivel
        d2 = d[d[niveles[i][1]].astype(str) == str(val)]
        if d2.empty:
            break
        validos.append(val)
        d = d2
    if len(validos) != len(path):
        _set_path(validos)
        path = validos

    nonce = st.session_state[k_nonce]
    nivel = len(path)
    etiqueta_niv, col_niv = niveles[nivel]
    es_hoja = nivel == len(niveles) - 1

    # --- Métrica principal + reiniciar -------------------------------------
    ops_met = ["Facturación", "Kilos"] + (["Contribución"] if mostrar_cm else [])
    c_met, c_res = st.columns([3, 1])
    met = c_met.radio("Métrica principal", ops_met, horizontal=True,
                      key=f"{key}_met")
    col_val, col_share = METRICAS_DRILL[met]
    c_res.markdown("<div style='height:1.7rem'></div>", unsafe_allow_html=True)
    if c_res.button("⟲ Reiniciar recorrido", key=f"{key}_reset",
                    disabled=not path, use_container_width=True):
        _set_path([])
        st.rerun()

    # --- Recorrido (migas): clic en una miga vuelve a ese nivel ------------
    st.caption("Recorrido: " + " → ".join(
        f"**{e}**" if i == nivel else e for i, (e, _) in enumerate(niveles)
    ))
    if path:
        cols_bc = st.columns(max(len(path), 4))
        for i, val in enumerate(path):
            _et = niveles[i][0]
            _txt = str(val) if len(str(val)) <= 22 else str(val)[:21] + "…"
            if cols_bc[i].button(
                f"✕ {_et}: {_txt}", key=f"{key}_bc_{i}",
                use_container_width=True,
                help=f"Quitar este nivel (y los siguientes) y volver a "
                     f"elegir {_et}.",
            ):
                _set_path(path[:i])
                st.rerun()

        # Métricas del contexto acumulado (todo lo seleccionado hasta acá).
        _fc = d["subtotalNeto"].sum()
        _kg = d["kilos"].sum()
        _fc_base = df_base["subtotalNeto"].sum()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Facturación", fmt_money(_fc))
        m1.caption(f"{(_fc / _fc_base * 100) if _fc_base else 0:.1f} % del total")
        m2.metric("Kilos", fmt_kg(_kg))
        if mostrar_cm:
            m3.metric("Contribución",
                      fmt_money(_fc - d["costo_unitario"].sum()))
        else:
            m3.metric("Precio medio",
                      (fmt_money(_fc / _kg) + " /kg") if _kg else "—")
        m4.metric("Clientes · SKUs",
                  f"{d['idCliente'].nunique()} · {d['idArticulo'].nunique()}")

    # --- Nivel actual -------------------------------------------------------
    g = dp.agrupar_dim(d, col_niv)
    g = g[g[col_niv].astype(str).str.strip() != ""]
    if g.empty:
        st.info(f"No hay {etiqueta_niv.lower()} para esta selección.")
        return None
    # Share de contribución: agrupar_dim no lo trae; se calcula acá.
    _tot_cm = g["cm"].sum()
    g["share_cm"] = (g["cm"] / _tot_cm * 100) if _tot_cm else 0.0
    g = g.sort_values(col_val, ascending=False).reset_index(drop=True)

    st.subheader(f"Nivel {nivel + 1} de {len(niveles)} · {etiqueta_niv}")

    # Tabla completa del nivel (va al Excel y a la vista final).
    _cf = [col_niv, "kilos", "share_kg", "subtotalNeto", "share_fc",
           "cm", "cm_pct", "precio_kg", "clientes", "skus"]
    if col_niv == "dsArticulo":
        _cf.remove("skus")  # cada fila ya ES un SKU
    t_full = g[_cols_cm(_cf)].rename(columns={col_niv: etiqueta_niv,
                                              **COLS_DIM})

    if es_hoja:
        # Último nivel: solo se muestra, no se navega más.
        if path:
            st.caption("Resultado filtrado por: " + " · ".join(
                f"{niveles[i][0]} = {v}" for i, v in enumerate(path)
            ))
        st.dataframe(t_full.style.format(FMT_DIM),
                     use_container_width=True, hide_index=True)
    else:
        st.caption(
            f"Hacé clic en una fila para abrir el siguiente nivel "
            f"(→ {niveles[nivel + 1][0]}). Part. % = participación dentro "
            f"de lo ya seleccionado."
        )
        c_graf, c_tab = st.columns([1.15, 1.45])
        with c_graf:
            st.plotly_chart(
                _barras_share(g, col_niv, etiqueta_niv, col_val, col_share),
                use_container_width=True,
                # key explícita: en el nivel 1 esta figura puede ser idéntica
                # al gráfico de composición de arriba y sin key Streamlit
                # colisiona los IDs autogenerados.
                key=f"{key}_fig_{nivel}_{nonce}",
            )
        with c_tab:
            _mn = COLS_DIM[col_val]  # nombre lindo de la métrica elegida
            t_click = g[[col_niv, col_val, col_share, "clientes",
                         "skus"]].rename(columns={
                col_niv: etiqueta_niv, col_val: _mn, col_share: "Part. %",
                "clientes": "Clientes", "skus": "SKUs",
            })
            ev = st.dataframe(
                t_click.style.format({_mn: FMT_DIM.get(_mn),
                                      "Part. %": fmt_pct}),
                use_container_width=True, hide_index=True,
                on_select="rerun", selection_mode="single-row",
                key=f"{key}_sel_{nivel}_{nonce}",
                height=min(420, 36 * (len(t_click) + 1) + 20),
            )
            filas = ev.selection.rows if ev and ev.selection else []
            if filas:
                _set_path(path + [str(g.iloc[filas[0]][col_niv])])
                st.rerun()
        with st.expander(f"Ver tabla completa del nivel ({len(t_full)})"):
            st.dataframe(t_full.style.format(FMT_DIM),
                         use_container_width=True, hide_index=True)

    return t_full


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

(tab_resumen, tab_lineas, tab_canales, tab_prod, tab_clientes,
 tab_vend, tab_alertas) = st.tabs(
    ["Resumen", "Proveedores", "Canales", "Productos (SKU)", "Clientes (RFM)",
     "Vendedores", "Alertas"]
)


# --- TAB RESUMEN ----------------------------------------------------------
with tab_resumen:
    m = dp.metricas_generales(df)

    # --- Proyección a fin de mes ------------------------------------------
    # Run-rate lineal: extrapola lo acumulado hasta hoy al total del mes,
    # usando el ritmo diario promedio. Cuenta solo días HÁBILES: los domingos
    # no se trabaja, así que no entran ni en los transcurridos ni en el total.
    # Tampoco cuentan los feriados listados en FERIADOS (no se factura).
    # Solo aplica al mes EN CURSO; los meses cerrados ya están completos.
    # TODO: por ahora sólo el 9 de julio; ampliar con el resto o una API.
    FERIADOS = {dt.date(2026, 7, 9)}

    def _dias_habiles(anio, mes, hasta_dia):
        """Días hábiles del 1 al hasta_dia (inclusive): no domingos ni feriados."""
        return sum(
            1 for d in range(1, hasta_dia + 1)
            if dt.date(anio, mes, d).weekday() != 6  # 6 = domingo
            and dt.date(anio, mes, d) not in FERIADOS
        )

    factor = 1.0
    proyectar = False
    if es_mes_actual:
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

    # Cada métrica: (etiqueta, valor a mostrar, valor para proyectar,
    # formato de proyección o None, escala). Las de CM solo se agregan para
    # el rol dueño, así supervisores nunca las reciben.
    metricas = [
        ("Facturación neta", fmt_money(m["subtotal_neto"]),
         m["subtotal_neto"], fmt_money, True),
        ("Kilos vendidos", fmt_kg(m["total_kilos"]),
         m["total_kilos"], fmt_kg, True),
    ]
    if mostrar_cm:
        metricas += [
            ("Contribución marginal", fmt_money(m["contribucion_marginal"]),
             m["contribucion_marginal"], fmt_money, True),
            ("CM %", fmt_pct(m["cm_pct"]),
             m["cm_pct"], fmt_pct, False),
        ]
    metricas += [
        ("Precio medio / kg", fmt_money(m["precio_medio_kg"]),
         m["precio_medio_kg"], fmt_money, False),
        ("Clientes únicos", f"{m['n_clientes']:,}".replace(",", "."),
         m["n_clientes"], _int, True),
        ("Ticket promedio", fmt_money(m["ticket_promedio"]),
         m["ticket_promedio"], fmt_money, False),
        ("SKUs vendidos", f"{m['n_skus']:,}".replace(",", "."),
         m["n_skus"], _int, True),
    ]

    # Tablas que junta esta solapa para el Excel descargable.
    hojas_resumen = {"KPIs": pd.DataFrame(
        [(lbl, pval) for lbl, _disp, pval, _pf, _e in metricas],
        columns=["Métrica", "Valor"],
    )}

    # Render en filas de 4 columnas.
    for i in range(0, len(metricas), 4):
        cols = st.columns(4)
        for col, (lbl, disp, pval, pfmt, escala) in zip(cols, metricas[i:i + 4]):
            col.metric(lbl, disp)
            if pfmt is not None:
                proy(col, pval, pfmt, escala)

    st.caption(
        f"{m['n_comprobantes']:,}".replace(",", ".") + " comprobantes  ·  "
        + fmt_kg(m["kg_por_cliente"]) + " por cliente (promedio)"
    )

    st.divider()

    # --- Evolución mensual (canal / subcanal / vendedor) --------------------
    # Usa la serie mensual histórica (data/serie_mensual.parquet),
    # INDEPENDIENTE del filtro de período: muestra todos los meses 2025–2026
    # para comparar.
    st.subheader("Evolución mensual")

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
            }
            # Supervisores no ven las métricas de CM en el selector.
            if mostrar_cm:
                METRICAS_EVOL["Contribución marginal (MB $)"] = ("cm", fmt_money)
                METRICAS_EVOL["CM % (margen)"] = ("cm_pct", fmt_pct)
            METRICAS_EVOL["Precio medio $/kg"] = ("precio_kg", fmt_money)

            c1, c2, c3 = st.columns([1.5, 0.9, 1.2])
            nombre_metrica = c1.selectbox(
                "Métrica", list(METRICAS_EVOL.keys()), index=0
            )
            nivel = c2.radio(
                "Abrir por", ["Canal", "Subcanal", "Vendedor"], horizontal=True
            )
            moneda = c3.radio(
                "Moneda", ["Corriente", "Constante (s/ inflación)"],
                horizontal=True,
                help="Corriente = pesos de cada mes (nominal). "
                     "Constante = todo llevado a pesos de hoy con el IPC del "
                     "INDEC, para comparar sin el efecto de la inflación.",
            )
            dim = {
                "Canal": "dsCanalMkt",
                "Subcanal": "dsSubcanalMKT",
                "Vendedor": "dsVendedor",
            }[nivel]

            # Respeta los filtros globales de canal/subcanal/vendedor si están
            # activos (las sumas crudas se re-agregan bien sea cual sea el
            # nivel elegido para abrir el gráfico).
            s = serie.copy()
            if seleccion.get("dsCanalMkt"):
                s = s[s["dsCanalMkt"].astype(str).str.strip()
                      .isin(seleccion["dsCanalMkt"])]
            if seleccion.get("dsSubcanalMKT"):
                s = s[s["dsSubcanalMKT"].astype(str).str.strip()
                      .isin(seleccion["dsSubcanalMKT"])]
            if seleccion.get("dsVendedor"):
                s = s[s["dsVendedor"].astype(str).str.strip()
                      .isin(seleccion["dsVendedor"])]

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
            # La línea de "Total" solo suma valor cuando se abre por Canal
            # (pocas categorías). En Subcanal/Vendedor hay demasiadas líneas
            # y el total se pisa con ellas, así que se omite.
            if nivel == "Canal":
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
                hojas_resumen["Evolución mensual"] = piv.reset_index()

    st.divider()
    boton_excel("resumen", hojas_resumen, key="xlsx_resumen")


# --- TAB LÍNEAS (gestión comercial por línea / marca) ----------------------
# Estructura en 3 niveles: panorama (qué pesa cada línea), apertura de una
# línea (por vendedor / canal / canal→vendedor / canal→subcanal) y la mirada
# inversa vendedor → línea → producto. Respeta los filtros globales de arriba:
# trabaja sobre el mismo `df` ya filtrado, así los shares se recalculan sobre
# la selección vigente.
with tab_lineas:
    # Línea "estricta": lookup por artículo; SKUs sin regla -> SIN ASIGNAR
    # (solo en esta solapa; en el resto siguen cayendo al proveedor).
    dfl = dp.agregar_linea_estricta(df)
    g_lin = dp.agrupar_dim(dfl, "linea_producto")
    hojas_lineas = {}  # tablas para el Excel descargable de la solapa

    # --- 1) Panorama: cuánto pesa cada línea --------------------------------
    st.subheader("Composición de la venta por línea de producto")

    _total_fc = g_lin["subtotalNeto"].sum()
    _fc_sin = g_lin.loc[
        g_lin["linea_producto"] == dp.SIN_ASIGNAR, "subtotalNeto"
    ].sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Líneas activas", f"{len(g_lin)}")
    c1.caption("con ventas en el período/filtros")
    c2.metric("Línea principal", g_lin.iloc[0]["linea_producto"])
    c2.caption(f"{g_lin.iloc[0]['share_fc']:.1f} % de la facturación")
    c3.metric("Top 3 líneas", fmt_pct(g_lin["share_fc"].head(3).sum()))
    c3.caption("de la facturación (concentración)")
    c4.metric("Sin asignar", fmt_money(_fc_sin))
    c4.caption(
        f"{(_fc_sin / _total_fc * 100) if _total_fc else 0:.1f} % de la "
        "facturación en SKUs sin línea"
    )

    met_lin = st.radio(
        "Ver composición por", ["Facturación", "Kilos"],
        horizontal=True, key="lin_met",
    )
    _cv = "subtotalNeto" if met_lin == "Facturación" else "kilos"
    _cs = "share_fc" if met_lin == "Facturación" else "share_kg"
    st.plotly_chart(
        _barras_share(g_lin, "linea_producto", "Línea", _cv, _cs),
        use_container_width=True,
    )

    with st.expander("Ver tabla completa de líneas"):
        _cols = _cols_cm(["linea_producto", "kilos", "share_kg", "subtotalNeto",
                          "share_fc", "cm", "cm_pct", "precio_kg", "clientes",
                          "skus"])
        t_lineas = g_lin[_cols].rename(
            columns={"linea_producto": "Línea", **COLS_DIM})
        st.dataframe(
            t_lineas.style.format(FMT_DIM),
            use_container_width=True, hide_index=True,
        )
        hojas_lineas["Líneas"] = t_lineas

    # --- 2) Navegación progresiva: Línea → Canal → ... → SKU -----------------
    st.divider()
    st.subheader("Navegación por línea")
    st.caption(
        "Elegí una línea y bajá nivel por nivel: Línea → Canal → Vendedor → "
        "Cliente → SKU. Cada nivel muestra solo lo compatible con lo ya "
        "seleccionado; con las etiquetas del recorrido volvés a un nivel "
        "anterior."
    )
    t_drill_lin = render_drill(
        dfl,
        [("Línea", "linea_producto"), ("Canal", "dsCanalMkt"),
         ("Vendedor", "dsVendedor"), ("Cliente", "nombreCliente"),
         ("SKU", "dsArticulo")],
        key="drill_prov",
    )
    if t_drill_lin is not None:
        hojas_lineas["Navegación"] = t_drill_lin

    # --- 3) SKUs sin línea asignada ------------------------------------------
    st.divider()
    _sin = dfl[dfl["linea_producto"] == dp.SIN_ASIGNAR]
    _n_sin = _sin["dsArticulo"].nunique()
    with st.expander(f"SKUs sin línea asignada ({_n_sin})"):
        if _sin.empty:
            st.success("Todos los SKUs del período tienen línea asignada.")
        else:
            g_sin = dp.agrupar_multi(_sin, ["dsArticulo", "proveedor"])
            _cols_s = ["dsArticulo", "proveedor", "kilos", "subtotalNeto",
                       "clientes"]
            t_sin = g_sin[_cols_s].rename(columns={
                "dsArticulo": "Producto", "proveedor": "Proveedor",
                **COLS_DIM,
            })
            st.dataframe(
                t_sin.style.format(FMT_DIM),
                use_container_width=True, hide_index=True,
            )
            hojas_lineas["SKUs sin línea"] = t_sin
            st.caption(
                "Para clasificarlos: agregar la fila correspondiente en "
                "data/marca_linea_lookup.csv (columnas dsArticulo,marca_linea) "
                "con el nombre EXACTO del artículo. El próximo run del "
                "pipeline los toma solo."
            )

    st.divider()
    boton_excel("proveedores", hojas_lineas, key="xlsx_lineas")


# --- TAB CANALES ----------------------------------------------------------
with tab_canales:
    st.subheader("Detalle por canal")
    t_canales = tabla_dim(dp.por_canal(df), "Canal", "dsCanalMkt",
                          mostrar_skus=True)

    # Para dueños: torta de share + CM % por canal lado a lado.
    # Para supervisores: solo la torta (a ancho completo), sin CM %.
    col_a, col_b = st.columns(2) if mostrar_cm else (st.container(), None)
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
    if mostrar_cm:
        with col_b:
            st.caption("CM % por canal")
            st.bar_chart(dp.por_canal(df).set_index("dsCanalMkt")["cm_pct"])

    st.divider()
    st.subheader("Detalle por subcanal")
    t_subcanales = tabla_dim(dp.por_subcanal(df), "Subcanal", "dsSubcanalMKT",
                             mostrar_skus=True)

    st.subheader("Detalle por marca / línea")
    t_marcas = tabla_dim(dp.por_proveedor(df), "Marca / Línea", "marca_linea")

    st.divider()
    boton_excel("canales", {
        "Canales": t_canales,
        "Subcanales": t_subcanales,
        "Marca - Línea": t_marcas,
    }, key="xlsx_canales")


# --- TAB PRODUCTOS (SKU) --------------------------------------------------
with tab_prod:
    prod = dp.ranking_productos(df)
    hojas_prod = {}  # tablas para el Excel descargable de la solapa

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
        # Filtro por clase ABC: si elegís "A" (o varias), el ranking de abajo
        # muestra el top SOLO de esa/esas clases. Vacío = todas. Sin esto,
        # el Top N siempre caía en clase A porque "prod" viene ordenado por
        # facturación descendente (los primeros N son casi siempre A).
        c_buscar, c_abc, c_top = st.columns([3, 1.4, 1.4])
        with c_buscar:
            buscar = st.text_input(
                "Buscar producto por nombre",
                placeholder="Buscar Producto",
                key="buscar_prod",
            )
        with c_abc:
            abc_uno = st.pills(
                "Clase ABC", ["A", "B", "C"], selection_mode="single",
                default=None, key="abc_sel_prod",
            )
        with c_top:
            top_n = st.select_slider(
                "Top N", options=[5, 10, 15, 25, 50], value=10,
                key="top_n_prod",
            )
        abc_sel = [abc_uno] if abc_uno else []
        prod_f = prod[prod["ABC"].isin(abc_sel)] if abc_sel else prod

        # Búsqueda por nombre: filtra el ranking por coincidencia parcial
        # (sin distinguir mayúsculas/acentos) en el nombre del producto.
        if buscar and buscar.strip():
            termino = buscar.strip()
            prod_f = prod_f[
                prod_f["dsArticulo"].astype(str)
                .str.normalize("NFKD").str.encode("ascii", "ignore").str.decode("ascii")
                .str.contains(
                    termino.encode("ascii", "ignore").decode("ascii"),
                    case=False, na=False,
                )
            ]

        titulo_clase = f" (clase {abc_uno})" if abc_uno else ""
        titulo_buscar = f' · "{buscar.strip()}"' if buscar and buscar.strip() else ""
        st.subheader(
            f"Ranking de productos con clasificación ABC{titulo_clase} · "
            f"Top {top_n}{titulo_buscar}"
        )
        if prod_f.empty:
            st.info("No hay productos en la clase seleccionada.")
        else:
            cols = ["dsArticulo", "ABC", "kilos", "subtotalNeto", "share_fc",
                    "cm", "cm_pct", "precio_kg", "clientes"]
            # Supervisores no ven Contribución ni CM % en el ranking de SKUs.
            if not mostrar_cm:
                cols = [c for c in cols if c not in ("cm", "cm_pct")]
            # Copia sin renombrar: sirve para recuperar el nombre real del
            # producto a partir de la fila que el usuario seleccione (la
            # selección devuelve la posición de la fila en este mismo orden).
            prod_top = prod_f[cols].head(top_n).reset_index(drop=True)
            # "clientes" = a cuántos clientes distintos se le vende el producto
            # (cobertura). COLS_DIM lo llama "Clientes"; acá lo mostramos como
            # "Cobertura" para dejar claro el sentido.
            t = prod_top.rename(columns={
                "dsArticulo": "Producto", **COLS_DIM, "clientes": "Cobertura",
            })
            # Al Excel va el ranking COMPLETO filtrado (no solo el Top N).
            hojas_prod["Ranking ABC"] = prod_f[cols].rename(columns={
                "dsArticulo": "Producto", **COLS_DIM, "clientes": "Cobertura",
            })
            sel_evt = st.dataframe(
                t.style.format(FMT_DIM), use_container_width=True,
                hide_index=True, on_select="rerun",
                selection_mode="single-row", key="tabla_prod",
            )
            st.caption(
                "Hacé clic en un producto para ver su apertura por canal, "
                "vendedor o cliente."
            )

            # --- Apertura del producto seleccionado --------------------------
            # Mismo formato que "Apertura de una línea" en la solapa Avance:
            # métricas del producto + apertura por dimensión a elección
            # (canal, vendedor, cliente o canal → cliente).
            filas_sel = sel_evt.selection.rows if sel_evt and sel_evt.selection else []
            if filas_sel:
                fila = filas_sel[0]
                nombre_prod = prod_top.iloc[fila]["dsArticulo"]
                det = df[df["dsArticulo"].astype(str) == str(nombre_prod)]
                if det.empty:
                    st.info("Sin datos para este producto.")
                else:
                    st.divider()
                    st.subheader(f"Navegación del producto · {nombre_prod}")

                    m_prod = prod_top.iloc[fila]
                    k1, k2, k3, k4 = st.columns(4)
                    k1.metric("Facturación", fmt_money(m_prod["subtotalNeto"]))
                    k1.caption(f"{m_prod['share_fc']:.1f} % del total")
                    k2.metric("Kilos", fmt_kg(m_prod["kilos"]))
                    k3.metric("Precio medio",
                              fmt_money(m_prod["precio_kg"]) + " /kg")
                    k4.metric("Cobertura", f"{int(m_prod['clientes'])} clientes")

                    st.caption(
                        "Bajá nivel por nivel: Canal → Vendedor → Cliente. "
                        "Al final ves qué clientes compraron este producto "
                        "con todos los filtros acumulados. Part. % = "
                        "participación dentro del producto y de lo ya "
                        "seleccionado."
                    )
                    # El recorrido se reinicia solo si se elige otro producto
                    # en el ranking (root_id).
                    t_drill_p = render_drill(
                        det,
                        [("Canal", "dsCanalMkt"),
                         ("Vendedor", "dsVendedor"),
                         ("Cliente", "nombreCliente")],
                        key="drill_prod", root_id=str(nombre_prod),
                    )
                    if t_drill_p is not None:
                        hojas_prod["Navegación producto"] = t_drill_p

    st.divider()
    boton_excel("productos", hojas_prod, key="xlsx_prod")


# --- TAB CLIENTES (RFM) ---------------------------------------------------
with tab_clientes:
    r = dp.rfm(df)
    hojas_cli = {}  # tablas para el Excel descargable de la solapa

    if r.empty:
        st.info("No hay datos suficientes para el RFM.")
    else:
        st.subheader("Segmentos de clientes (RFM)")
        seg = dp.resumen_segmentos(r)
        t_seg = seg.rename(columns={
            "segmento": "Segmento", "clientes": "Clientes",
            "facturacion": "Facturación",
        })
        st.dataframe(
            t_seg.style.format({"Facturación": fmt_money}),
            use_container_width=True, hide_index=True,
        )
        hojas_cli["Segmentos"] = t_seg

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
        # Al Excel va la lista COMPLETA de clientes del filtro (no el Top N).
        if not r_f.empty:
            hojas_cli["Clientes RFM"] = (
                r_f.sort_values("monetario", ascending=False)
                [["nombreCliente", "segmento", "monetario", "frecuencia",
                  "recencia"]]
                .rename(columns={
                    "nombreCliente": "Cliente", "segmento": "Segmento",
                    "monetario": "Facturación", "frecuencia": "Frecuencia",
                    "recencia": "Recencia (días)",
                })
            )
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

    # --- Altas y bajas de clientes ------------------------------------------
    # Compara el MES ELEGIDO arriba contra SU mes anterior (necesita ver los
    # dos meses a la vez, por eso usa el parquet completo y no df_periodo).
    # Los filtros de dimensión (canal, vendedor, etc.) sí aplican.
    st.divider()
    st.subheader("Altas y bajas de clientes")

    _df_ab = cargar_datos_local(os.path.getmtime(PARQUET_PATH))
    for _c_ab, _v_ab in seleccion.items():
        if _v_ab:
            _df_ab = _df_ab[_df_ab[_c_ab].astype(str).str.strip().isin(_v_ab)]

    _mes_ant_ab = desde - dt.timedelta(days=1)  # último día del mes anterior
    _f_ab = _df_ab["fechaComprobate"]
    _hay_ant = (
        (_f_ab >= pd.Timestamp(_mes_ant_ab.replace(day=1)))
        & (_f_ab < pd.Timestamp(desde))
    ).any()

    if not _hay_ant:
        st.info(
            f"No hay datos de {_mes_ant_ab:%m/%Y} en el parquet, así que no "
            f"se puede comparar {hasta:%m/%Y} contra su mes anterior."
        )
    else:
        # hoy=hasta: para el mes en curso corta en hoy; para un mes cerrado
        # usa el mes completo. Así funciona con cualquier mes de 2026.
        altas, bajas = dp.altas_bajas(_df_ab, hoy=hasta)

        _nota_curso = (
            f" (al {hasta:%d/%m/%Y}, puede revertirse si compran antes de "
            f"fin de mes)" if es_mes_actual else ""
        )
        st.caption(
            f"Altas: compraron en {hasta:%m/%Y} y no en {_mes_ant_ab:%m/%Y}. "
            f"Bajas: compraron en {_mes_ant_ab:%m/%Y} y no en "
            f"{hasta:%m/%Y}{_nota_curso}."
        )

        _cols_ab = ["nombreCliente", "compras", "kilos", "facturacion",
                    "ultima_compra"]
        _ren_ab = {
            "nombreCliente": "Cliente", "compras": "Compras", "kilos": "Kilos",
            "facturacion": "Facturación", "ultima_compra": "Última compra",
        }
        _fmt_ab = {
            "Kilos": fmt_kg, "Facturación": fmt_money,
            "Última compra": lambda x: f"{x:%d/%m/%Y}",
        }

        col_alta, col_baja = st.columns(2)
        with col_alta:
            st.metric("Altas", len(altas))
            if altas.empty:
                st.info("Sin altas en el mes seleccionado.")
            else:
                t_altas = altas[_cols_ab].rename(columns=_ren_ab)
                st.dataframe(
                    t_altas.style.format(_fmt_ab),
                    use_container_width=True, hide_index=True,
                )
                hojas_cli["Altas"] = t_altas
        with col_baja:
            st.metric("Bajas", len(bajas))
            if bajas.empty:
                st.info("Sin bajas: todos los clientes del mes anterior "
                        "volvieron a comprar.")
            else:
                t_bajas = bajas[_cols_ab].rename(columns=_ren_ab)
                st.dataframe(
                    t_bajas.style.format(_fmt_ab),
                    use_container_width=True, hide_index=True,
                )
                hojas_cli["Bajas"] = t_bajas
        st.caption(
            "Las cifras de cada tabla corresponden al mes en que el cliente "
            "compró (altas: mes seleccionado · bajas: su mes anterior)."
        )

    st.divider()
    boton_excel("clientes", hojas_cli, key="xlsx_clientes")


# --- TAB VENDEDORES -------------------------------------------------------
with tab_vend:
    st.subheader("Detalle por vendedor")
    t_vend = tabla_dim(dp.por_vendedor(df), "Vendedor", "dsVendedor",
                       mostrar_skus=True, mostrar_skus_cliente=True)

    st.caption("Facturación por vendedor")
    st.bar_chart(dp.por_vendedor(df).set_index("dsVendedor")["subtotalNeto"])

    st.divider()
    boton_excel("vendedores", {"Vendedores": t_vend}, key="xlsx_vend")


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

        st.divider()
        boton_excel(
            "alertas",
            {"Alertas": pd.DataFrame(avisos).rename(
                columns={"nivel": "Nivel", "texto": "Alerta"})},
            key="xlsx_alertas",
        )
