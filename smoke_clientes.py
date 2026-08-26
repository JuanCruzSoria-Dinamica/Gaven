"""Smoke test de la solapa CLIENTES (tabla bruta a nivel cliente).

La solapa muestra UNA fila por cliente con operaciones en el mes elegido.
Lo que no puede pasar nunca:

  1. Que dé distinto al Resumen. Los totales de kilos / facturación /
     contribución de la tabla tienen que ser IDÉNTICOS a
     dp.metricas_generales() sobre el mismo df.
  2. Que un cliente aparezca dos veces (la tabla se ordena y se filtra en
     Excel: un cliente repetido se lee como dos clientes).
  3. Que "Compras" cuente renglones de artículo en vez de comprobantes.
  4. Que la última compra caiga fuera del período.
  5. Que la cobertura de SKUs se vaya de 0-100 % (el período está contenido
     en la ventana del universo, así que no puede pasar del 100 %).
  6. Que el refactor de dominante() haya cambiado altas_bajas().
  7. Que se rompa con bordes: df vacío, columna faltante, cliente con solo
     notas de crédito (kilos <= 0 -> nada de división por cero).

No escribe nada: es de solo lectura.

Uso:  python3 smoke_clientes.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import datetime as dt                      # noqa: E402
import numpy as np                         # noqa: E402
import pandas as pd                        # noqa: E402
import data_pipeline as dp                 # noqa: E402

fallas = 0


def chk(cond, msg, detalle=""):
    global fallas
    print(("  ok    " if cond else "  FALLA ") + msg)
    if detalle:
        print("          " + str(detalle))
    if not cond:
        fallas += 1


def casi(a, b, tol=1e-6):
    """Compara flotantes con tolerancia relativa (sumas de ~10^8)."""
    a, b = float(a), float(b)
    escala = max(abs(a), abs(b), 1.0)
    return abs(a - b) / escala < tol


print("=" * 72)
print("SMOKE · solapa Clientes")
print("=" * 72)

if not os.path.exists(dp.PARQUET_PATH):
    print("No hay parquet en %s. Corré el pipeline primero." % dp.PARQUET_PATH)
    sys.exit(1)

df_todo = pd.read_parquet(dp.PARQUET_PATH)
df_todo["fechaComprobate"] = pd.to_datetime(df_todo["fechaComprobate"])
meses = sorted(df_todo["fechaComprobate"].dt.to_period("M").astype(str).unique())
print("Parquet: %d filas · meses %s → %s" % (len(df_todo), meses[0], meses[-1]))


# ---------------------------------------------------------------------------
# 1) Contra el parquet real, mes por mes
# ---------------------------------------------------------------------------
for mes in meses:
    print("\n--- %s ---" % mes)
    anio, m = int(mes[:4]), int(mes[5:7])
    desde = dt.date(anio, m, 1)
    f = df_todo["fechaComprobate"]
    d = df_todo[(f.dt.year == anio) & (f.dt.month == m)].copy()
    hasta = d["fechaComprobate"].max().date()

    g = dp.por_cliente(d)

    # -- una fila por cliente --
    chk(len(g) == d["idCliente"].nunique(),
        "una fila por cliente (%d filas / %d clientes)"
        % (len(g), d["idCliente"].nunique()))
    chk(not g["idCliente"].duplicated().any(), "sin idCliente repetido")

    # -- los totales cierran con el Resumen --
    met = dp.metricas_generales(d)
    chk(casi(g["kilos"].sum(), met["total_kilos"]),
        "kilos = metricas_generales", "%.2f vs %.2f" % (g["kilos"].sum(), met["total_kilos"]))
    chk(casi(g["subtotalNeto"].sum(), met["subtotal_neto"]),
        "facturación = metricas_generales")
    chk(casi(g["cm"].sum(), met["contribucion_marginal"]),
        "contribución = metricas_generales")

    # -- compras = comprobantes del cliente, no renglones --
    # NO se compara contra metricas_generales["n_comprobantes"]: el ERP REUSA
    # el nrodoc entre clientes distintos, así que dp.comprobante_id
    # (empresa|documento|nrodoc) COLISIONA y el conteo GLOBAL queda
    # subestimado (ver el aviso del final). A nivel cliente no hay colisión
    # -- eso es justo lo que se verifica acá, contra una clave a prueba de
    # colisiones que le agrega la fecha y el idCliente.
    _k = (dp.comprobante_id(d) + "|"
          + d["fechaComprobate"].dt.strftime("%Y-%m-%d") + "|"
          + d["idCliente"].astype(str))
    esperado = d.assign(_k=_k).groupby("idCliente")["_k"].nunique()
    chk((g.set_index("idCliente")["compras"]
         == esperado.reindex(g["idCliente"]).values).all(),
        "compras = comprobantes del cliente (clave sin colisiones)")
    chk(int(g["compras"].sum()) == int(esperado.sum()),
        "las compras suman los comprobantes reales del mes (%d)"
        % int(esperado.sum()))
    chk(int(g["skus"].max()) <= met["n_skus"], "SKUs por cliente <= SKUs totales")

    # -- última compra dentro del período --
    chk(g["ultima_compra"].min().date() >= desde
        and g["ultima_compra"].max().date() <= hasta,
        "última compra dentro del período (%s → %s)" % (desde, hasta))

    # -- CM % y precio/kg consistentes con sus componentes --
    con_fc = g[g["subtotalNeto"] != 0]
    chk(np.allclose(con_fc["cm_pct"],
                    con_fc["cm"] / con_fc["subtotalNeto"] * 100),
        "CM % = cm / facturación")
    con_kg = g[g["kilos"] != 0]
    chk(np.allclose(con_kg["precio_kg"],
                    con_kg["subtotalNeto"] / con_kg["kilos"]),
        "precio/kg = facturación / kilos")
    chk(np.isfinite(g["precio_kg"]).all() and np.isfinite(g["cm_pct"]).all(),
        "sin inf/NaN en precio/kg ni CM % (clientes con kilos o fact. 0)")

    # -- atributos siempre presentes --
    for col in dp.CLIENTE_ATRIBUTOS:
        chk(g[col].notna().all() and (g[col].astype(str).str.len() > 0).all(),
            "%s sin vacíos" % col)

    # -- el dominante es realmente el de mayor facturación --
    for col in dp.CLIENTE_ATRIBUTOS:
        real = (d.assign(**{col: d[col].astype(str).str.strip()})
                  .groupby(["idCliente", col])["subtotalNeto"].sum()
                  .reset_index().sort_values("subtotalNeto", ascending=False)
                  .drop_duplicates("idCliente").set_index("idCliente")[col])
        # se saca la marca "(+n)" antes de comparar
        etiq = g.set_index("idCliente")[col].str.replace(
            r"\s\(\+\d+\)$", "", regex=True)
        chk((etiq == real.reindex(etiq.index)).all(),
            "%s = dominante por facturación" % col)

    # -- la marca (+n) coincide con la cantidad real de valores --
    n_vend = d.groupby("idCliente")["dsVendedor"].nunique()
    marcados = set(g.loc[g["dsVendedor"].str.contains(r"\(\+\d+\)$", regex=True),
                         "idCliente"])
    esperados = set(n_vend[n_vend > 1].index)
    chk(marcados == esperados,
        "marca (+n) en los %d clientes con más de un vendedor" % len(esperados))

    # -- cobertura de SKUs entre 0 y 100 --
    univ_desde = dp.inicio_universo(desde)
    fu = df_todo["fechaComprobate"]
    d_univ = df_todo[(fu >= pd.Timestamp(univ_desde))
                     & (fu < pd.Timestamp(hasta) + pd.Timedelta(days=1))]
    gc = dp.agregar_cobertura(dp.por_cliente(d), "idCliente", d_univ)
    chk("cob_skus" in gc.columns, "agregar_cobertura devuelve cob_skus")
    if "cob_skus" in gc.columns:
        v = gc["cob_skus"].dropna()
        chk(len(v) > 0 and v.between(0, 100).all(),
            "cob_skus dentro de 0-100 %%" % () if not len(v) else
            "cob_skus dentro de 0-100 %% (min %.1f / max %.1f)"
            % (v.min(), v.max()))
    chk("cob_clientes" not in gc.columns,
        "NO trae cob_clientes (a nivel cliente sería siempre 100 %)")


# ---------------------------------------------------------------------------
# 2) El refactor de dominante() no cambió altas_bajas()
# ---------------------------------------------------------------------------
print("\n--- refactor dominante() ---")
_ult = df_todo["fechaComprobate"].max().date()
altas, bajas = dp.altas_bajas(df_todo, hoy=_ult)
chk(len(altas) or len(bajas), "altas_bajas sigue devolviendo datos",
    "altas=%d bajas=%d" % (len(altas), len(bajas)))
chk(altas["dsVendedor"].notna().all() and altas["dsCanalMkt"].notna().all(),
    "altas_bajas conserva canal y vendedor")
chk(not altas["idCliente"].duplicated().any(), "altas sin cliente repetido")

_d_mes = df_todo[df_todo["fechaComprobate"].dt.to_period("M").astype(str) == meses[-1]]
chk(dp.dominante(_d_mes, "dsVendedor") is not None, "dominante() a nivel módulo")
chk(dp.dominante(_d_mes, "columna_que_no_existe") is None,
    "dominante() con columna inexistente devuelve None")


# ---------------------------------------------------------------------------
# 3) Bordes: nada de esto puede tirar excepción (rompería el tablero entero)
# ---------------------------------------------------------------------------
print("\n--- bordes ---")
try:
    vacio = dp.por_cliente(df_todo.iloc[0:0])
    chk(len(vacio) == 0, "df vacío -> tabla vacía, sin excepción")
    chk("nombreCliente" in vacio.columns, "df vacío -> conserva las columnas")
except Exception as e:                                   # noqa: BLE001
    chk(False, "df vacío no explota", e)

try:
    chk(len(dp.por_cliente(None)) == 0, "None -> tabla vacía")
except Exception as e:                                   # noqa: BLE001
    chk(False, "None no explota", e)

try:
    sin_region = _d_mes.drop(columns=["region"])
    g2 = dp.por_cliente(sin_region)
    chk((g2["region"] == dp.CLIENTE_SIN_DATO).all(),
        "sin columna 'region' -> '(sin dato)', no explota")
except Exception as e:                                   # noqa: BLE001
    chk(False, "columna faltante no explota", e)

try:
    neg = _d_mes[_d_mes["subtotalNeto"] < 0]
    if len(neg):
        g3 = dp.por_cliente(neg)
        chk(np.isfinite(g3["precio_kg"]).all(),
            "solo notas de crédito -> sin inf en precio/kg (%d filas)" % len(neg))
    else:
        print("  --    no hay filas negativas en el último mes, se saltea")
except Exception as e:                                   # noqa: BLE001
    chk(False, "notas de crédito no explotan", e)


# ---------------------------------------------------------------------------
# 4) AVISO (no es falla de esta solapa): dp.comprobante_id colisiona
# ---------------------------------------------------------------------------
# empresa|documento|nrodoc NO identifica un comprobante: el ERP reusa el
# nrodoc para clientes distintos en fechas distintas. No afecta a la solapa
# Clientes (dentro de un mismo cliente no hay colisión, se verifica arriba),
# pero SÍ subestima n_comprobantes y por lo tanto INFLA el ticket promedio
# del Resumen. Se reporta como aviso para decidirlo aparte.
print("\n--- aviso: colisiones de comprobante_id ---")
_c = dp.comprobante_id(df_todo)
_r = (_c + "|" + df_todo["fechaComprobate"].dt.strftime("%Y-%m-%d")
      + "|" + df_todo["idCliente"].astype(str))
_a, _b = _c.nunique(), _r.nunique()
print("  comprobantes con la clave actual : %d" % _a)
print("  comprobantes con la clave robusta: %d  (%+.1f %%)"
      % (_b, (_b - _a) / _a * 100))
print("  -> el ticket promedio del Resumen queda inflado en ese %.")
print("     No afecta a la solapa Clientes.")

print("\n" + "=" * 72)
print("FALLAS: %d" % fallas)
print("=" * 72)
sys.exit(1 if fallas else 0)
