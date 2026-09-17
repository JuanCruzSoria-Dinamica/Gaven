"""Smoke test de la BONIFICACIÓN McCAIN POR CANAL (dp.aplicar_bonificacion_canal).

McCain le hace a Gaven un 10% sobre el precio de compra de tres SKUs, pero SOLO
para el canal GRANJAS. Chess no lo puede cargar: ahí el precio de compra es por
artículo, así que el descuento se le aplicaría también a FOOD SERVICE,
MAYORISTAS y RETAIL, que venden los mismos SKUs sin la bonificación. Por eso se
resta del costo al leer, igual que los acuerdos del Excel.

Chequea:
  1. Alcance: solo el canal y los SKUs de la regla; el resto queda intacto.
  2. Cuenta: ajuste = pct x costo bruto, en pesable (por kilo) y no pesable
     (por bulto).
  3. Es ADICIONAL a los acuerdos y NO se calcula sobre el costo ya neto: los
     dos ajustes se restan de la base bruta y el orden no cambia el resultado.
  4. La mercadería bonificada (sin cargo) no lleva costo, así que tampoco
     descuento.
  5. Las notas de crédito revierten el ajuste solas (cantidades negativas).
  6. Vigencia: 'desde' / 'hasta' recortan por mes.
  7. Bordes: df vacío, sin reglas, detalle sin las columnas de cargo.
  8. Datos reales: impacto por canal y por mes, y que solo se mueva GRANJAS.

No escribe nada.

Uso:  python3 smoke_bonif_canal.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd                        # noqa: E402
import data_pipeline as dp                 # noqa: E402

fallas = 0
SKU = 81109          # uno de los SKUs de la regla
SKU_FUERA = 99999    # un SKU cualquiera que no está en la regla


def chk(cond, msg, detalle=""):
    global fallas
    print(("  ok    " if cond else "  FALLA ") + msg)
    if detalle:
        print("         " + detalle)
    if not cond:
        fallas += 1


def det(filas):
    """Detalle mínimo con las columnas que necesitan costo y bonificación.

    (fecha, canal, idArticulo, cantidadesTotal, pesoTotal, unimedtotal,
     unimedcargo, preciocomprant, subtotalNeto)
    """
    d = pd.DataFrame(filas, columns=[
        "fechaComprobate", "dsCanalMkt", "idArticulo", "cantidadesTotal",
        "pesoTotal", "unimedtotal", "unimedcargo", "preciocomprant",
        "subtotalNeto",
    ])
    d["fechaComprobate"] = pd.to_datetime(d["fechaComprobate"])
    d["idCliente"] = 1
    return dp.recalcular_costo(d)


REGLA = [{"nombre": "test", "canal": "GRANJAS", "articulos": (SKU,),
          "pct": 0.10, "desde": None, "hasta": None}]

# --- 1. Alcance -------------------------------------------------------------
print("\n[1] Alcance: canal y SKU")
# No pesable: 10 bultos, 100 kg, precio de compra 1.000 por bulto -> costo 10.000
base = det([
    ("2026-08-05", "GRANJAS",      SKU,       10, 0, 100, 100, 1000, 20000),
    ("2026-08-05", "FOOD SERVICE", SKU,       10, 0, 100, 100, 1000, 20000),
    ("2026-08-05", "GRANJAS",      SKU_FUERA, 10, 0, 100, 100, 1000, 20000),
])
r = dp.aplicar_bonificacion_canal(base, REGLA)
chk(r.loc[0, "ajuste_bonif_canal"] == 1000,
    "GRANJAS + SKU de la regla: descuenta el 10%",
    f"ajuste = {r.loc[0, 'ajuste_bonif_canal']:.0f} sobre un costo de 10.000")
chk(r.loc[1, "ajuste_bonif_canal"] == 0,
    "mismo SKU en FOOD SERVICE: sin descuento (es lo que Chess no sabe hacer)")
chk(r.loc[2, "ajuste_bonif_canal"] == 0,
    "otro SKU en GRANJAS: sin descuento")
chk(r.loc[0, "costo_unitario"] == 9000 and r.loc[1, "costo_unitario"] == 10000,
    "el descuento se resta de costo_unitario (de ahí salen CM y CM%)")
chk((r["dsCanalMkt"] == base["dsCanalMkt"]).all() and len(r) == len(base),
    "no agrega ni pierde filas")

# --- 2. Pesable vs no pesable -----------------------------------------------
print("\n[2] La cuenta, en pesable y no pesable")
# Pesable: 250 kg a 40 por kilo -> costo 10.000
p = det([("2026-08-05", "GRANJAS", SKU, 10, 250, 250, 250, 40, 20000)])
rp = dp.aplicar_bonificacion_canal(p, REGLA)
chk(rp.loc[0, "Categoria"] == "Pesable" and rp.loc[0, "ajuste_bonif_canal"] == 1000,
    "pesable: 10% del precio de compra x kilos cobrados")
chk(r.loc[0, "Categoria"] == "No Pesable",
    "no pesable: 10% del precio de compra x bultos cobrados")

# --- 3. Adicional a los acuerdos, no sobre el costo ya neto -----------------
print("\n[3] Convive con los acuerdos McCain")
ac = pd.DataFrame([{"anio": 2026, "mes": 8, "idCliente": 1,
                    "idArticulo": SKU, "desc_kg": 20.0}])
uno = det([("2026-08-05", "GRANJAS", SKU, 10, 250, 250, 250, 40, 20000)])
# acuerdo: 20 $/kg x 250 kg = 5.000. bonificación: 10% de 10.000 = 1.000.
a_luego_b = dp.aplicar_bonificacion_canal(dp.aplicar_acuerdos(uno, ac), REGLA)
b_luego_a = dp.aplicar_acuerdos(dp.aplicar_bonificacion_canal(uno, REGLA), ac)
chk(round(a_luego_b.loc[0, "costo_unitario"], 6) == 4000,
    "costo = bruto - acuerdo - bonificación (10.000 - 5.000 - 1.000 = 4.000)")
chk(round(a_luego_b.loc[0, "costo_unitario"], 6)
    == round(b_luego_a.loc[0, "costo_unitario"], 6),
    "el orden no cambia el resultado: los dos salen de la base bruta")
chk(round(a_luego_b.loc[0, "ajuste_bonif_canal"], 6) == 1000,
    "la bonificación NO se calcula sobre el costo ya neto de acuerdos",
    "si se calculara sobre el neto daría 500, no 1.000")

# --- 4. Mercadería bonificada (sin cargo) -----------------------------------
print("\n[4] Mercadería sin cargo")
sc = det([("2026-08-05", "GRANJAS", SKU, 0, 0, 100, 0, 1000, 0)])
rsc = dp.aplicar_bonificacion_canal(sc, REGLA)
chk(rsc.loc[0, "costo_unitario"] == 0 and rsc.loc[0, "ajuste_bonif_canal"] == 0,
    "sin cargo no hay costo, así que tampoco descuento (lo repone el proveedor)")

# --- 5. Notas de crédito ----------------------------------------------------
print("\n[5] Devoluciones")
nc = det([
    ("2026-08-05", "GRANJAS", SKU,  10, 0, 100, 100, 1000, 20000),
    ("2026-08-20", "GRANJAS", SKU, -10, 0, -100, -100, 1000, -20000),
])
rnc = dp.aplicar_bonificacion_canal(nc, REGLA)
chk(round(rnc["ajuste_bonif_canal"].sum(), 6) == 0,
    "la NC revierte el ajuste de la venta que anula")
chk(round(rnc["costo_unitario"].sum(), 6) == 0,
    "y el costo del par venta+devolución vuelve a cero")

# --- 6. Vigencia ------------------------------------------------------------
print("\n[6] Vigencia por mes")
meses = det([
    ("2026-07-15", "GRANJAS", SKU, 10, 0, 100, 100, 1000, 20000),
    ("2026-08-15", "GRANJAS", SKU, 10, 0, 100, 100, 1000, 20000),
    ("2026-09-15", "GRANJAS", SKU, 10, 0, 100, 100, 1000, 20000),
])
rv = dp.aplicar_bonificacion_canal(
    meses, [{**REGLA[0], "desde": "2026-08", "hasta": "2026-08"}])
chk(list(rv["ajuste_bonif_canal"] > 0) == [False, True, False],
    "'desde' y 'hasta' recortan por mes, inclusive")

# --- 7. Bordes --------------------------------------------------------------
print("\n[7] Bordes")
vacio = det([]).iloc[0:0]
rvac = dp.aplicar_bonificacion_canal(vacio, REGLA)
chk("ajuste_bonif_canal" in rvac.columns and rvac.empty,
    "df vacío: devuelve la columna igual (la app la usa siempre)")
rsin = dp.aplicar_bonificacion_canal(base, [])
chk((rsin["ajuste_bonif_canal"] == 0).all()
    and (rsin["costo_unitario"] == base["costo_unitario"]).all(),
    "sin reglas: no toca nada")
crudo = base.drop(columns=["kilos_cargo", "bultos_cargo", "Categoria",
                           "costo_unitario"])
rcrudo = dp.aplicar_bonificacion_canal(crudo, REGLA)
chk(rcrudo.loc[0, "ajuste_bonif_canal"] == 1000,
    "detalle sin las columnas de cargo: las re-deriva solo")
dos = dp.aplicar_bonificacion_canal(dp.aplicar_bonificacion_canal(base, REGLA),
                                    REGLA)
chk(dos.loc[0, "costo_unitario"] == 8000,
    "NO es idempotente: aplicarla dos veces descuenta dos veces",
    "por eso se llama en un solo lugar (cargar_datos_local), como los acuerdos")

# --- 8. Datos reales --------------------------------------------------------
print("\n[8] Detalle real en disco")
if os.path.exists(dp.PARQUET_PATH):
    real = pd.read_parquet(dp.PARQUET_PATH)
    real = dp.recalcular_costo(real)
    real = dp.agregar_marca_linea(real)
    antes = dp.aplicar_acuerdos(real)
    desp = dp.aplicar_bonificacion_canal(antes)

    tocadas = desp["ajuste_bonif_canal"] != 0
    canales = set(desp.loc[tocadas, "dsCanalMkt"].astype(str).str.strip())
    arts = set(desp.loc[tocadas, "idArticulo"])
    chk(canales <= {r["canal"] for r in dp.BONIF_CANAL_REGLAS},
        f"solo se tocan los canales de la regla: {sorted(canales)}")
    chk(arts <= {a for r in dp.BONIF_CANAL_REGLAS for a in r["articulos"]},
        f"solo los SKUs de la regla: {sorted(arts)}")
    chk((desp["ajuste_bonif_canal"] >= 0).sum() + (desp["ajuste_bonif_canal"] < 0).sum()
        == len(desp), "sin nulos en el ajuste")

    a = antes.copy(); d = desp.copy()
    for x in (a, d):
        x["canal"] = x["dsCanalMkt"].astype(str).str.strip()
        x["mes"] = x["fechaComprobate"].dt.to_period("M").astype(str)
    mc_a = a[a["marca_linea"] == "MC CAIN FOOD"]
    mc_d = d[d["marca_linea"] == "MC CAIN FOOD"]
    g = pd.DataFrame({
        "fact": mc_a.groupby(["mes", "canal"])["subtotalNeto"].sum(),
        "costo_antes": mc_a.groupby(["mes", "canal"])["costo_unitario"].sum(),
        "costo_desp": mc_d.groupby(["mes", "canal"])["costo_unitario"].sum(),
    })
    g["CM% antes"] = (g.fact - g.costo_antes) / g.fact * 100
    g["CM% después"] = (g.fact - g.costo_desp) / g.fact * 100
    g["puntos"] = g["CM% después"] - g["CM% antes"]
    tabla = g[["CM% antes", "CM% después", "puntos"]].round(2)
    ult = tabla.loc[sorted(set(i[0] for i in tabla.index))[-3:]]
    print("\n         MC CAIN FOOD · CM% por canal, últimos meses")
    print("         " + ult.to_string().replace("\n", "\n         "))
    otros = tabla[tabla.index.get_level_values("canal") != "GRANJAS"]
    chk((otros["puntos"].abs() < 1e-9).all(),
        "los demás canales no se mueven ni un decimal")
    gr = tabla[tabla.index.get_level_values("canal") == "GRANJAS"]
    chk((gr["puntos"] > 0).all(), "GRANJAS sube en todos los meses")
else:
    print("  (sin parquet de detalle: se omite)")

print("\n" + ("TODO OK" if not fallas else f"{fallas} FALLA(S)"))
sys.exit(1 if fallas else 0)
