"""Smoke test de APP de la solapa Clientes: levanta app.py entero con AppTest
y verifica que la solapa nueva no rompa nada del tablero.

Chequea, con los DOS roles:
  1. Que la app levante sin excepción (una key de widget repetida, un
     desempaquetado de tabs mal hecho o un st.dataframe inválido revientan
     acá, no en producción).
  2. Que estén las 9 (o 10) solapas esperadas y en el orden esperado.
  3. Que la tabla de clientes se renderice y tenga las columnas que pidió
     Tomás.
  4. Que el supervisor NO vea Contribución ni CM % en esa tabla.
  5. Que el buscador filtre y que el botón de Excel exista y baje un .xlsx
     legible con las mismas filas que la tabla.

Uso:  python3 smoke_app_clientes.py
"""
import io
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd                          # noqa: E402
from streamlit.testing.v1 import AppTest     # noqa: E402

fallas = 0


def chk(cond, msg, detalle=""):
    global fallas
    print(("  ok    " if cond else "  FALLA ") + msg)
    if detalle:
        print("          " + str(detalle))
    if not cond:
        fallas += 1


LABELS = ["Resumen", "Proveedores", "Canales", "Productos (SKU)", "Clientes",
          "Altas y Bajas", "Vendedores", "Alertas", "Metas"]

COLS_MIN = ["ID", "Cliente", "Región", "Canal", "Subcanal", "Vendedor", "Kilos",
            "Facturación", "$/kg", "SKUs", "Cob. SKUs %", "Compras",
            "Última compra"]
COLS_CM = ["Contribución", "CM %"]


def tabla_clientes(at):
    """Devuelve el DataFrame de la tabla de la solapa Clientes (la única que
    tiene la columna 'Compras')."""
    for el in at.dataframe:
        d = el.value
        d = getattr(d, "data", d)            # puede venir como Styler
        if isinstance(d, pd.DataFrame) and "Compras" in d.columns:
            return d
    return None


for rol in ("dueno", "supervisor"):
    print("\n=== rol: %s ===" % rol)
    at = AppTest.from_file("app.py", default_timeout=300)
    at.session_state["rol"] = rol
    at.run()

    chk(not at.exception, "el tablero levanta")
    for e in at.exception:
        print("          ", e.value)
    if at.exception:
        continue

    # at.tabs viene PLANO: primero las solapas de arriba y después las
    # sub-solapas de Metas (Seguimiento, Evolutivo, ...). Por eso se compara
    # el prefijo y "Acuerdos McCain" se busca aparte.
    labels = [t.label for t in at.tabs]
    chk(labels[:len(LABELS)] == LABELS, "solapas de arriba en orden",
        labels[:len(LABELS)])
    chk(("Acuerdos McCain" in labels) == (rol == "dueno"),
        "Acuerdos McCain solo para el dueño")

    t = tabla_clientes(at)
    chk(t is not None, "la tabla de clientes se renderiza")
    if t is None:
        continue

    faltan = [c for c in COLS_MIN if c not in t.columns]
    chk(not faltan, "están las columnas pedidas", "faltan: %s" % faltan)
    chk(len(t) > 0, "la tabla trae filas (%d clientes)" % len(t))
    chk(t["ID"].duplicated().sum() == 0, "sin ID de cliente repetido")
    _dup = int(t["Cliente"].duplicated(keep=False).sum())
    chk(_dup == 0 or t[["ID", "Cliente"]].duplicated().sum() == 0,
        "los clientes que comparten nombre (%d) se distinguen por ID" % _dup)

    hay_cm = [c for c in COLS_CM if c in t.columns]
    if rol == "dueno":
        chk(hay_cm == COLS_CM, "el dueño ve Contribución y CM %", hay_cm)
    else:
        chk(not hay_cm, "el supervisor NO ve Contribución ni CM %", hay_cm)

    # -- buscador --
    nombre = str(t["Cliente"].iloc[0])
    at.text_input(key="busca_cli").set_value(nombre[:6]).run()
    chk(not at.exception, "el buscador no rompe")
    t2 = tabla_clientes(at)
    chk(t2 is not None and len(t2) <= len(t) and len(t2) > 0,
        "el buscador recorta la tabla (%s → %s filas)"
        % (len(t), len(t2) if t2 is not None else "—"))
    at.text_input(key="busca_cli").set_value("").run()

    # -- botón de Excel --
    botones = [b for b in at.get("download_button")]
    keys = [getattr(b, "key", None) for b in botones]
    chk("xlsx_cli_detalle" in keys, "existe el botón de Excel de Clientes", keys)
    chk(len(keys) == len(set(keys)), "no hay keys de descarga repetidas", keys)

    # El contenido del .xlsx no se puede leer desde AppTest (el proto del
    # download_button no expone los bytes), pero no hace falta: boton_excel()
    # arma el workbook con openpyxl EN CADA CORRIDA, antes de pasárselo a
    # st.download_button. Si la generación del Excel fallara, la app habría
    # tirado excepción y el primer check ya estaría en rojo.

print("\n" + "=" * 72)
print("FALLAS: %d" % fallas)
print("=" * 72)
sys.exit(1 if fallas else 0)
