"""Smoke test de las "Lecturas por canal (mesa chica)" de la solapa Alertas.

Dos partes:
  1. La lógica pura (dp.insights_mesa_chica) contra el parquet real: que salgan
     las 6 lecturas por canal y que las cifras coincidan con el cálculo hecho
     a mano sobre el detalle.
  2. La app entera con AppTest: que la solapa renderice sin excepciones.

No escribe nada: es de solo lectura.

Uso:  python3 smoke_insights.py
"""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd                        # noqa: E402
import data_pipeline as dp                 # noqa: E402

fallas = 0


def chk(cond, msg):
    global fallas
    print(("  ok    " if cond else "  FALLA ") + msg)
    if not cond:
        fallas += 1


print("\nSmoke test · Lecturas por canal (mesa chica)")

det = dp.agregar_marca_linea(pd.read_parquet(dp.PARQUET_PATH))
det["fechaComprobate"] = pd.to_datetime(det["fechaComprobate"])
ult = det["fechaComprobate"].max().date()
desde = ult.replace(day=1)

# --- 1. Ventana de comparación ---------------------------------------------
ini_prev, fin_prev = dp.ventana_anterior(desde, ult)
chk(fin_prev.day == ult.day, "el mes anterior se corta al mismo día del mes")
chk(ini_prev.month == (desde.month - 1 or 12), "la ventana previa es el mes anterior")

# Mes corto: 31/03 contra febrero no puede pedir el 31 de febrero.
_i, _f = dp.ventana_anterior(dt.date(2026, 3, 1), dt.date(2026, 3, 31))
chk((_i, _f) == (dt.date(2026, 2, 1), dt.date(2026, 2, 28)),
    "un mes más corto que el corte se recorta a su último día")

# --- 2. Cobertura: 6 lecturas por canal ------------------------------------
ins = dp.insights_mesa_chica(det, desde, ult)
chk(not ins.empty, "devuelve lecturas para el período en curso")

canales = sorted(
    dp._recortar(det, desde, ult)["dsCanalMkt"].dropna().astype(str)
    .str.strip().replace({"": None}).dropna().unique().tolist()
)
chk(sorted(ins["Canal"].unique()) == canales,
    f"sale un bloque por cada canal con venta ({len(canales)} canales)")

for c in canales:
    hay = set(ins[ins["Canal"] == c]["Insight"])
    faltan = [n for n in dp.INSIGHTS_ORDEN if n not in hay]
    chk(not faltan, f"{c}: están las 6 lecturas" + (f" (faltan {faltan})" if faltan else ""))

chk(set(ins["nivel"]) <= {"ok", "riesgo"}, "el nivel de cada tarjeta es ok/riesgo")
chk(ins["Detalle"].str.len().min() > 0, "ninguna lectura queda sin explicación")

# --- 3. Las cifras cierran contra el detalle -------------------------------
act = dp._recortar(det, desde, ult)
prev = dp._recortar(det, ini_prev, fin_prev)

for c in canales:
    a = act[act["dsCanalMkt"].astype(str).str.strip() == c]
    p = prev[prev["dsCanalMkt"].astype(str).str.strip() == c]
    bloque = ins[ins["Canal"] == c].set_index("Insight")

    # Vendedor estrella = el de mayor facturación neta del canal.
    esperado = a.groupby("dsVendedor")["subtotalNeto"].sum().idxmax()
    chk(bloque.loc["Vendedor estrella", "Protagonista"] == esperado,
        f"{c}: vendedor estrella = {esperado}")

    # Producto estrella = el SKU de mayor facturación neta.
    esperado = a.groupby("dsArticulo")["subtotalNeto"].sum().idxmax()
    chk(bloque.loc["Producto estrella", "Protagonista"] == esperado,
        f"{c}: producto estrella = {esperado}")

    # Producto con más clientes = el de mayor cantidad de clientes únicos.
    cli = a.groupby("dsArticulo")["idCliente"].nunique()
    chk(bloque.loc["Producto con más clientes", "Protagonista"]
        in set(cli[cli == cli.max()].index),
        f"{c}: producto con más clientes tiene {int(cli.max())} clientes")

    # Producto caído = mayor caída ABSOLUTA de facturación mes vs mes.
    if not p.empty and "Producto caído" in bloque.index:
        fa = a.groupby("dsArticulo")["subtotalNeto"].sum()
        fp = p.groupby("dsArticulo")["subtotalNeto"].sum()
        dif = (fa.reindex(fp.index).fillna(0) - fp)
        chk(bloque.loc["Producto caído", "Protagonista"] == dif.idxmin(),
            f"{c}: producto caído = {dif.idxmin()}")
        chk(dif.min() < 0, f"{c}: el producto caído efectivamente cayó")

# --- 4. Casos borde --------------------------------------------------------
chk(dp.insights_mesa_chica(det.head(0), desde, ult).empty,
    "con df vacío devuelve tabla vacía (no rompe)")

enero = dp.insights_mesa_chica(det, dt.date(2026, 1, 1), dt.date(2026, 1, 31))
sin_caidos = enero[enero["Insight"].isin(["Producto caído", "Cliente caído"])]
chk(sin_caidos.empty,
    "enero (sin mes anterior en el parquet) no inventa lecturas de caída")

un_canal = dp.insights_mesa_chica(det, desde, ult, canales=[canales[0]])
chk(set(un_canal["Canal"]) == {canales[0]}, "se puede pedir un solo canal")

# --- 5. La app renderiza ---------------------------------------------------
try:
    from streamlit.testing.v1 import AppTest

    at = AppTest.from_file("app.py", default_timeout=180)
    at.session_state["rol"] = "dueno"   # saltea el login, igual que smoke_metas
    at.run()
    chk(not at.exception, "app.py corre sin excepciones")
    for e in at.exception:
        print("        ", e.value)

    textos = " ".join(m.value for m in at.markdown if isinstance(m.value, str))
    chk("Lecturas por canal" in " ".join(
        s.value for s in at.subheader) or "Lecturas por canal" in textos,
        "la solapa Alertas muestra la sección de lecturas por canal")
    chk("ins-card" in textos, "las tarjetas se renderizan")
except ImportError:
    print("  aviso: streamlit no instalado, se saltea el render de la app")

print(f"\n{'TODO OK' if not fallas else str(fallas) + ' FALLA(S)'}\n")
sys.exit(1 if fallas else 0)
