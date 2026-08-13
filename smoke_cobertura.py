"""Smoke test de la COBERTURA (universo del año como denominador).

Cobertura = de todo lo que se le podía vender, cuánto se le vendió. El
denominador es el universo: clientes y SKUs distintos de TODO el año. El
numerador sale del período elegido (un mes).

Chequea:
  1. dp.universo_dim / dp.agregar_cobertura / dp.cobertura_total contra el
     parquet real, con el cálculo hecho a mano al lado.
  2. Las invariantes que no se pueden violar nunca: el período está contenido
     en el año, así que la cobertura siempre cae entre 0 y 100 %.
  3. Los bordes: universo vacío, columna inexistente, df sin filas. Nada de
     esto puede tirar una excepción (el tablero se rompería entero).

No escribe nada: es de solo lectura.

Uso:  python3 smoke_cobertura.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

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

det = pd.read_parquet(dp.PARQUET_PATH)
det["fechaComprobate"] = pd.to_datetime(det["fechaComprobate"])

# Universo = todo el año (mismo criterio que app.py).
uni = det[det["fechaComprobate"].dt.year == dp.ANIO]

# Período de prueba: el último mes CERRADO que haya en el parquet. Se usa un
# mes cerrado a propósito: el mes en curso da cobertura baja por definición
# (van pocos días) y no sirve para validar nada.
_ult = uni["fechaComprobate"].max()
_ini_actual = _ult.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
_ini_prev = (_ini_actual - pd.Timedelta(days=1)).replace(day=1)
per = uni[(uni["fechaComprobate"] >= _ini_prev)
          & (uni["fechaComprobate"] < _ini_actual)]

print(f"  (universo: {dp.ANIO} completo · período: {_ini_prev:%m/%Y})")

chk(not per.empty, "hay un mes cerrado para probar")

# --- 1. Nivel empresa (tarjetas del Resumen) --------------------------------
tot = dp.cobertura_total(per, uni)

chk(tot["universo_clientes"] == uni["idCliente"].nunique(),
    "el universo de clientes es el nunique del año",
    f"{tot['universo_clientes']} clientes")
chk(tot["universo_skus"] == uni["idArticulo"].nunique(),
    "el universo de SKUs es el nunique del año",
    f"{tot['universo_skus']} SKUs")
chk(tot["clientes"] == per["idCliente"].nunique(),
    "los clientes del período son el nunique del mes")

_esp = per["idCliente"].nunique() / uni["idCliente"].nunique() * 100
chk(abs(tot["cob_clientes"] - _esp) < 1e-9,
    "la cobertura de clientes es período / universo",
    f"{tot['cob_clientes']:.1f} %")

# --- 2. Invariantes ---------------------------------------------------------
# El período es un subconjunto del año, así que el numerador NUNCA puede
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

# Un vendedor dado de baja (tiene cartera en el año pero no vendió en el mes)
# tiene que aparecer con 0 %, no desaparecer de la tabla.
_sin_venta = set(uni["dsVendedor"].unique()) - set(per["dsVendedor"].unique())
if _sin_venta:
    print(f"  (vendedores sin ventas en {_ini_prev:%m/%Y}: {len(_sin_venta)})")

print("\n" + "=" * 60)
if fallas:
    print(f"{fallas} FALLA(S)")
    sys.exit(1)
print("smoke test OK")
