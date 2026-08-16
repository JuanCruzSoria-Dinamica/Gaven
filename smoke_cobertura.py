"""Smoke test de la COBERTURA (universo = últimos 3 meses).

Cobertura = de todo lo que se le podía vender, cuánto se le vendió. El
denominador es el universo: clientes y SKUs distintos de los últimos
dp.VENTANA_UNIVERSO_MESES meses (el mes del período + los 2 anteriores). El
numerador sale del período elegido (un mes).

Chequea:
  0. dp.inicio_universo: la aritmética de la ventana móvil, incluido el
     cruce de año.
  1. dp.universo_dim / dp.agregar_cobertura / dp.cobertura_total contra el
     parquet real, con el cálculo hecho a mano al lado.
  2. Las invariantes que no se pueden violar nunca: el período está contenido
     en la ventana, así que la cobertura siempre cae entre 0 y 100 %.
  3. Que la ventana de 3 meses sea efectivamente MÁS CHICA que el año (si no,
     el cambio de criterio no se aplicó).
  4. Los bordes: universo vacío, columna inexistente, df sin filas. Nada de
     esto puede tirar una excepción (el tablero se rompería entero).

No escribe nada: es de solo lectura.

Uso:  python3 smoke_cobertura.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import datetime as dt                      # noqa: E402
import pandas as pd                        # noqa: E402
import data_pipeline as dp                 # noqa: E402

fallas = 0


def chk(cond, msg, detalle=""):
    global fallas
    print(("  ok    " if cond else "  FALLA ") + msg)
    if detalle:
        print("         " + detalle)
    if not cond:
        fallas += 1


print("\nSmoke test · Cobertura")

# --- 0. La aritmética de la ventana móvil -----------------------------------
# Se prueba sola, sin datos: es puro calendario y tiene que cruzar el año bien.
chk(dp.inicio_universo(dt.date(2026, 6, 1)) == dt.date(2026, 4, 1),
    "inicio_universo(jun 2026) = abr 2026 (abr + may + jun)")
chk(dp.inicio_universo(dt.date(2026, 1, 1)) == dt.date(2025, 11, 1),
    "inicio_universo(ene 2026) = nov 2025 (cruza el año)")
chk(dp.inicio_universo(dt.date(2026, 3, 1)) == dt.date(2026, 1, 1),
    "inicio_universo(mar 2026) = ene 2026")
chk(dp.inicio_universo(dt.date(2026, 6, 15)) == dt.date(2026, 4, 1),
    "inicio_universo normaliza al día 1 aunque le pasen otro día")
chk(dp.inicio_universo(dt.date(2026, 6, 1), meses=1) == dt.date(2026, 6, 1),
    "con meses=1 la ventana es solo el mes elegido")
chk(dp.inicio_universo(None) is None, "inicio_universo(None) no rompe")

det = pd.read_parquet(dp.PARQUET_PATH)
det["fechaComprobate"] = pd.to_datetime(det["fechaComprobate"])

# Período de prueba: el último mes CERRADO que haya en el parquet. Se usa un
# mes cerrado a propósito: el mes en curso da cobertura baja por definición
# (van pocos días) y no sirve para validar nada.
_ult = det["fechaComprobate"].max()
_ini_actual = _ult.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
_ini_prev = (_ini_actual - pd.Timedelta(days=1)).replace(day=1)
per = det[(det["fechaComprobate"] >= _ini_prev)
          & (det["fechaComprobate"] < _ini_actual)]

# Universo = ventana móvil de 3 meses que TERMINA en el mes del período
# (mismo criterio que app.py). El corte superior es el fin del período, no
# "hoy": si no, el numerador no quedaría contenido en el denominador.
_ini_uni = pd.Timestamp(dp.inicio_universo(_ini_prev.date()))
uni = det[(det["fechaComprobate"] >= _ini_uni)
          & (det["fechaComprobate"] < _ini_actual)]

print(f"  (universo: {_ini_uni:%m/%Y} → {_ini_prev:%m/%Y} "
      f"· {dp.VENTANA_UNIVERSO_MESES} meses · período: {_ini_prev:%m/%Y})")

chk(not per.empty, "hay un mes cerrado para probar")
chk(not uni.empty, "la ventana del universo tiene datos")

# --- 1. Nivel empresa (tarjetas del Resumen) --------------------------------
tot = dp.cobertura_total(per, uni)

chk(tot["universo_clientes"] == uni["idCliente"].nunique(),
    "el universo de clientes es el nunique de la ventana de 3 meses",
    f"{tot['universo_clientes']} clientes")
chk(tot["universo_skus"] == uni["idArticulo"].nunique(),
    "el universo de SKUs es el nunique de la ventana de 3 meses",
    f"{tot['universo_skus']} SKUs")
chk(tot["clientes"] == per["idCliente"].nunique(),
    "los clientes del período son el nunique del mes")

_esp = per["idCliente"].nunique() / uni["idCliente"].nunique() * 100
chk(abs(tot["cob_clientes"] - _esp) < 1e-9,
    "la cobertura de clientes es período / universo",
    f"{tot['cob_clientes']:.1f} %")

# --- 2. Invariantes ---------------------------------------------------------
# El período es un subconjunto de la ventana, así que el numerador NUNCA puede
# superar al denominador. Si esto falla, alguien mezcló los dos df.
for k in ("cob_clientes", "cob_skus"):
    chk(0 <= tot[k] <= 100, f"{k} cae entre 0 y 100 %", f"{tot[k]:.1f} %")

for col, lab in [("dsCanalMkt", "canal"), ("dsVendedor", "vendedor"),
                 ("marca_linea", "marca / línea")]:
    g = dp.agregar_cobertura(dp.agrupar_dim(per, col), col, uni)

    chk("cob_clientes" in g.columns and "cob_skus" in g.columns,
        f"por {lab}: aparecen las columnas de cobertura")

    _mal_cli = g[g["clientes"] > g["universo_clientes"]]
    chk(_mal_cli.empty,
        f"por {lab}: ningún grupo tiene más clientes que su universo",
        "" if _mal_cli.empty else f"{len(_mal_cli)} fila(s) rotas")

    _mal_sku = g[g["skus"] > g["universo_skus"]]
    chk(_mal_sku.empty,
        f"por {lab}: ningún grupo tiene más SKUs que su universo")

    _fuera = g[(g["cob_clientes"] < 0) | (g["cob_clientes"] > 100)]
    chk(_fuera.empty, f"por {lab}: todas las coberturas caen entre 0 y 100 %")

    chk(g["universo_clientes"].notna().all(),
        f"por {lab}: ningún grupo se quedó sin universo (merge completo)")

# --- 3. Un caso puntual a mano ----------------------------------------------
# Se recalcula la cobertura del vendedor más grande sin usar las funciones,
# para que el test no se valide a sí mismo.
g_v = dp.agregar_cobertura(dp.agrupar_dim(per, "dsVendedor"), "dsVendedor", uni)
_top = g_v.sort_values("subtotalNeto", ascending=False).iloc[0]
_v = _top["dsVendedor"]
_uc = uni[uni["dsVendedor"] == _v]["idCliente"].nunique()
_pc = per[per["dsVendedor"] == _v]["idCliente"].nunique()
chk(abs(_top["cob_clientes"] - _pc / _uc * 100) < 1e-9,
    f"a mano: {_v} cubrió {_pc} de {_uc} clientes",
    f"{_top['cob_clientes']:.1f} %")

# --- 3 bis. El criterio nuevo vs. el viejo ----------------------------------
# Guardia de regresión del cambio de criterio: la ventana de 3 meses tiene que
# dar un universo MÁS CHICO que el año entero (y por lo tanto una cobertura
# MÁS ALTA). Si estos dos números empiezan a coincidir, alguien volvió a
# enchufar el año como denominador en app.py.
_uni_anio = det[(det["fechaComprobate"].dt.year == dp.ANIO)
                & (det["fechaComprobate"] < _ini_actual)]
_tot_anio = dp.cobertura_total(per, _uni_anio)

chk(tot["universo_clientes"] <= _tot_anio["universo_clientes"],
    "la cartera de 3 meses no supera a la del año",
    f"{tot['universo_clientes']} vs {_tot_anio['universo_clientes']} del año")
chk(tot["cob_clientes"] >= _tot_anio["cob_clientes"],
    "la cobertura con ventana corta es mayor o igual que con el año",
    f"{tot['cob_clientes']:.1f} % vs {_tot_anio['cob_clientes']:.1f} %")

if _uni_anio["fechaComprobate"].min() < _ini_uni:
    chk(tot["universo_clientes"] < _tot_anio["universo_clientes"],
        "hay meses fuera de la ventana: el universo se achicó de verdad",
        f"quedaron afuera "
        f"{_tot_anio['universo_clientes'] - tot['universo_clientes']} clientes")


# --- 4. Bordes: nada de esto puede explotar ---------------------------------
_vacio = uni.iloc[0:0]

g_sin_uni = dp.agregar_cobertura(dp.agrupar_dim(per, "dsCanalMkt"),
                                 "dsCanalMkt", _vacio)
chk("cob_clientes" not in g_sin_uni.columns,
    "sin universo devuelve la tabla intacta (no inventa columnas)")

chk(dp.universo_dim(_vacio, "dsCanalMkt").empty,
    "universo_dim con df vacío devuelve vacío")
chk(dp.universo_dim(uni, "columna_que_no_existe").empty,
    "universo_dim con una columna inexistente no rompe")
chk(dp.universo_dim(None, "dsCanalMkt").empty,
    "universo_dim con None no rompe")

_tot_vacio = dp.cobertura_total(_vacio, _vacio)
chk(_tot_vacio["cob_clientes"] == 0 and _tot_vacio["cob_skus"] == 0,
    "cobertura_total sin datos devuelve 0 (no divide por cero)")

# Período vacío contra universo lleno: 0 % y sin excepción.
_tot_per_vacio = dp.cobertura_total(_vacio, uni)
chk(_tot_per_vacio["cob_clientes"] == 0,
    "un período sin ventas da 0 % de cobertura")

# Un vendedor con cartera en la ventana pero sin ventas en el mes tiene que
# aparecer con 0 %, no desaparecer de la tabla.
_sin_venta = set(uni["dsVendedor"].unique()) - set(per["dsVendedor"].unique())
if _sin_venta:
    print(f"  (vendedores sin ventas en {_ini_prev:%m/%Y}: {len(_sin_venta)})")

print("\n" + "=" * 60)
if fallas:
    print(f"{fallas} FALLA(S)")
    sys.exit(1)
print("smoke test OK")
