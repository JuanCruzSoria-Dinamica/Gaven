"""Smoke test del de-para Chess ↔ Tablero (conciliacion_chess.py).

Arma un mes de mentira con una fila por cada motivo de exclusión y chequea:
  1. Que la cascada cierre: crudo - lo que saca cada filtro = tablero.
  2. Que el resultado de la cascada sea EXACTAMENTE el de dp.preparar()
     (el control que corre el script contra datos reales).
  3. Que las definiciones de kilos separen bien pesables de no pesables.
  4. Que la tabla por vendedor marque la diferencia donde tiene que estar.

No toca la API ni el parquet: es de solo lectura y todo sintético.

Uso:  python3 smoke_conciliacion.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd                        # noqa: E402
import data_pipeline as dp                 # noqa: E402
import conciliacion_chess as cc            # noqa: E402

fallas = 0


def chk(cond, msg):
    global fallas
    print(("  ok    " if cond else "  FALLA ") + msg)
    if not cond:
        fallas += 1


def fila(**kw):
    """Fila cruda de la API con valores por defecto que PASAN los filtros."""
    base = dict(
        dsEmpresa="GRILUDELL SRL", dsDocumento="FACTURA A", nrodoc="1",
        anulado="NO", fechaComprobate="2026-08-05",
        idCliente=500, nombreCliente="CLIENTE SA",
        dsLocalidad="TIGRE", dsProvincia="BUENOS AIRES",
        idVendedor=3, dsVendedor="FENLEY, MARCELO",
        idCanalMkt=1, dsCanalMkt="FOOD SERVICE",
        idSubcanalMkt=1, dsSubcanalMKT="RESTAURANTES",
        idArticulo=123, dsArticulo="PAPA BASTON", dsTipoMercaderia="CONGELADO",
        proveedor="9 - MC CAIN", cantidadesTotal=10, peso=10,
        pesoTotal=100.0, unimedtotal=110.0, unimedcargo=110.0,
        precioUnitarioNeto=1000.0, subtotalNeto=10000.0,
        subtotalFinal=12100.0, preciocomprant=500.0,
    )
    base.update(kw)
    return base


print("\nSmoke test · De-para Chess ↔ Tablero")

crudo_api = pd.DataFrame([
    fila(),                                              # queda
    fila(nrodoc="2", pesoTotal=0.0, unimedtotal=40.0,    # queda, no pesable
         unimedcargo=40.0, idArticulo=124),
    fila(nrodoc="3", anulado="SI"),                      # sale: anulado
    fila(nrodoc="4", dsCanalMkt="VIANDAS"),              # sale: canal
    fila(nrodoc="5", dsVendedor="DIRECTA"),              # sale: vendedor
    fila(nrodoc="6", dsSubcanalMKT="VIANDAS"),           # sale: subcanal
    fila(nrodoc="7", idCliente=dp.CLIENTES_EXCLUIR[0]),  # sale: cliente
    fila(nrodoc="8", idArticulo=dp.ARTICULOS_EXCLUIR[0]),  # sale: artículo
])

crudo = cc.normalizar_crudo(crudo_api)
casc, tablero = cc.cascada(crudo)

# --- 1. La cascada cierra ---------------------------------------------------
for m in cc.METRICAS:
    neto = casc.loc[casc["Paso"] != "TABLERO", m].sum()
    chk(abs(neto - casc.loc[casc["Paso"] == "TABLERO", m].iloc[0]) < 1e-6,
        f"la cascada cierra en '{m}' (crudo menos filtros = tablero)")

chk(len(tablero) == 2, "quedan solo las 2 filas buenas (6 salen por filtro)")
chk(all(casc.loc[casc["Paso"].str.startswith("  (-)"), "Filas"] == -1),
    "cada filtro se lleva exactamente su fila (ninguno pisa a otro)")

# --- 2. Igual que el pipeline de verdad -------------------------------------
real = dp.preparar(crudo_api)
chk(len(real) == len(tablero), "misma cantidad de filas que dp.preparar()")
chk(abs(real["kilos"].sum() - tablero["kilos"].sum()) < 1e-6,
    "mismos kilos que dp.preparar()")
chk(abs(real["subtotalNeto"].sum() - tablero["subtotalNeto"].sum()) < 1e-6,
    "misma facturación que dp.preparar()")

# --- 3. Las definiciones de kilos -------------------------------------------
d = cc.definiciones_de_kilos(tablero).set_index("Definicion")
um = d.loc[d.index.str.startswith("unimedtotal"), "Total"].iloc[0]
peso = d.loc[d.index.str.startswith("pesoTotal"), "Total"].iloc[0]
kil = d.loc[d.index.str.startswith("kilos"), "Total"].iloc[0]
chk(abs(um - 150.0) < 1e-6, "unimedtotal suma lo que muestra Chess (110 + 40)")
chk(abs(peso - 100.0) < 1e-6, "pesoTotal solo cuenta el artículo que pesa")
chk(abs(kil - 140.0) < 1e-6,
    "kilos del tablero = peso del pesable (100) + unimed del no pesable (40)")
chk(kil != um, "kilos del tablero y UM de Chess NO son la misma columna")

# --- 4. Tabla por vendedor --------------------------------------------------
tv = cc.por_vendedor(crudo, tablero).set_index("Vendedor")
chk(abs(tv.loc["DIRECTA", "UM Chess"] - 110.0) < 1e-6,
    "DIRECTA aparece en la columna Chess")
chk(tv.loc["DIRECTA", "UM Tablero"] == 0,
    "DIRECTA queda en cero del lado del tablero")
chk(abs(tv.loc["DIRECTA", "Dif UM"] + 110.0) < 1e-6,
    "la diferencia de DIRECTA sale con signo negativo")

# --- 5. Aviso de prefijos ---------------------------------------------------
chk(cc.revisar_prefijos(crudo) == [],
    "sin prefijos numéricos no avisa nada")
con_pref = crudo.copy()
con_pref["dsVendedor"] = "14 - DIRECTA"
avisos = cc.revisar_prefijos(con_pref)
chk(any(col == "dsVendedor" for col, _ in avisos),
    "si la API trae '14 - DIRECTA' lo avisa (el filtro no estaría sacando)")

print(f"\n{'TODO OK' if not fallas else str(fallas) + ' FALLA(S)'}")
sys.exit(1 if fallas else 0)
