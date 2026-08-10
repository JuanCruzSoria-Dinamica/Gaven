"""Tests del sistema de metas (canal -> proveedor -> vendedor).

Corre contra archivos temporales, no toca data/metas.parquet.
Uso:  python3 test_metas.py
"""
import datetime as dt
import os
import sys
import tempfile

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import data_pipeline as dp  # noqa: E402

FALLOS = []
OK = 0


def chk(cond, msg):
    global OK
    if cond:
        OK += 1
        print(f"  ok   {msg}")
    else:
        FALLOS.append(msg)
        print(f"  FALLA {msg}")


def casi(a, b, tol=0.51):
    return abs(float(a) - float(b)) <= tol


# ---------------------------------------------------------------------------
tmp = tempfile.mkdtemp(prefix="metas_test_")
P = os.path.join(tmp, "metas.parquet")
H = os.path.join(tmp, "hist.parquet")


def upsert(df, mes, canal, tipo="objetivo", nivel="proveedor"):
    return dp.upsert_metas(df, anio_mes=mes, canales=[canal], tipo=tipo,
                           nivel=nivel, path=P, historial=False)


print("\n[1] Esquema y normalización")

vacio = dp.cargar_metas(path=os.path.join(tmp, "no_existe.parquet"))
chk(list(vacio.columns) == dp.METAS_COLS, "cargar_metas sin archivo devuelve el esquema completo")
chk(vacio.empty, "cargar_metas sin archivo devuelve vacío")

# Blanquea la etiqueta que no corresponde al nivel.
sucio = pd.DataFrame([{
    "anio_mes": "2026-09", "tipo": "objetivo", "nivel": "canal",
    "dsCanalMkt": " FOOD ", "marca_linea": "RESTO DE OTRA GRILLA",
    "dsVendedor": "ALGUIEN", "meta_kg": 100.0,
}])
n = dp.normalizar_metas(sucio)
chk(n["marca_linea"].iloc[0] == "" and n["dsVendedor"].iloc[0] == "",
    "nivel 'canal' blanquea marca_linea y dsVendedor")
chk(n["dsCanalMkt"].iloc[0] == "FOOD", "hace strip del canal")

# Filas sin la etiqueta que el nivel exige se descartan.
sin_marca = pd.DataFrame([{"anio_mes": "2026-09", "tipo": "objetivo",
                           "nivel": "proveedor", "dsCanalMkt": "FOOD",
                           "marca_linea": "", "dsVendedor": "", "meta_kg": 50}])
chk(dp.normalizar_metas(sin_marca).empty,
    "nivel 'proveedor' sin marca se descarta")

sin_vend = pd.DataFrame([{"anio_mes": "2026-09", "tipo": "objetivo",
                          "nivel": "vendedor", "dsCanalMkt": "FOOD",
                          "marca_linea": "", "dsVendedor": "", "meta_kg": 50}])
chk(dp.normalizar_metas(sin_vend).empty,
    "nivel 'vendedor' sin vendedor se descarta")

cero = pd.DataFrame([{"anio_mes": "2026-09", "tipo": "objetivo",
                      "nivel": "proveedor", "dsCanalMkt": "FOOD",
                      "marca_linea": "X", "dsVendedor": "", "meta_kg": 0}])
chk(dp.normalizar_metas(cero).empty, "meta en 0 equivale a no tener meta")

dup = pd.DataFrame([
    {"anio_mes": "2026-09", "tipo": "objetivo", "nivel": "proveedor",
     "dsCanalMkt": "FOOD", "marca_linea": "X", "dsVendedor": "", "meta_kg": 30},
    {"anio_mes": "2026-09", "tipo": "objetivo", "nivel": "proveedor",
     "dsCanalMkt": "FOOD", "marca_linea": "X", "dsVendedor": "", "meta_kg": 20},
])
chk(casi(dp.normalizar_metas(dup)["meta_kg"].sum(), 50) and len(dp.normalizar_metas(dup)) == 1,
    "duplicados de la misma clave se suman en una fila")

basura = pd.DataFrame([{"anio_mes": "2026-09", "tipo": "inventado",
                        "nivel": "proveedor", "dsCanalMkt": "FOOD",
                        "marca_linea": "X", "dsVendedor": "", "meta_kg": 10}])
chk(dp.normalizar_metas(basura).empty, "tipo desconocido se descarta")


print("\n[2] Migración del parquet viejo")

viejo = pd.DataFrame([
    {"anio_mes": "2026-08", "dsCanalMkt": "FOOD", "marca_linea": "MCCAIN",
     "meta_kg": 1000.0},
])
pv = os.path.join(tmp, "viejo.parquet")
viejo.to_parquet(pv, index=False)
mig = dp.cargar_metas(path=pv)
chk(list(mig.columns) == dp.METAS_COLS, "el parquet viejo se lee con el esquema nuevo")
chk(mig["tipo"].iloc[0] == "objetivo" and mig["nivel"].iloc[0] == "proveedor",
    "las filas viejas quedan como objetivo/proveedor")
chk(casi(mig["meta_kg"].iloc[0], 1000), "la migración no pierde kilos")
mig2 = dp.cargar_metas(path=pv)
chk(mig2.equals(mig), "la migración es idempotente")


print("\n[3] Upsert acotado a (mes, tipo, nivel, canal)")

if os.path.exists(P):
    os.remove(P)

prov = pd.DataFrame([
    {"anio_mes": "2026-09", "dsCanalMkt": "FOOD", "marca_linea": "MCCAIN", "meta_kg": 6000},
    {"anio_mes": "2026-09", "dsCanalMkt": "FOOD", "marca_linea": "GARCIA", "meta_kg": 4000},
])
upsert(prov, "2026-09", "FOOD", nivel="proveedor")
upsert(pd.DataFrame([{"anio_mes": "2026-09", "dsCanalMkt": "FOOD", "meta_kg": 10000}]),
       "2026-09", "FOOD", nivel="canal")
vend = pd.DataFrame([
    {"anio_mes": "2026-09", "dsCanalMkt": "FOOD", "dsVendedor": "AHMED", "meta_kg": 7000},
    {"anio_mes": "2026-09", "dsCanalMkt": "FOOD", "dsVendedor": "ROJAS", "meta_kg": 3000},
])
upsert(vend, "2026-09", "FOOD", nivel="vendedor")

m = dp.cargar_metas(path=P)
chk(casi(dp.total_meta(m, "2026-09", "objetivo", "canal", "FOOD"), 10000),
    "guardar el nivel vendedor no pisó el nivel canal")
chk(casi(dp.total_meta(m, "2026-09", "objetivo", "proveedor", "FOOD"), 10000),
    "guardar el nivel vendedor no pisó el nivel proveedor")
chk(casi(dp.total_meta(m, "2026-09", "objetivo", "vendedor", "FOOD"), 10000),
    "el nivel vendedor quedó guardado")

# Otro canal no se toca.
upsert(pd.DataFrame([{"anio_mes": "2026-09", "dsCanalMkt": "GRANJAS",
                      "marca_linea": "MCCAIN", "meta_kg": 500}]),
       "2026-09", "GRANJAS", nivel="proveedor")
m = dp.cargar_metas(path=P)
chk(casi(dp.total_meta(m, "2026-09", "objetivo", "proveedor", "FOOD"), 10000),
    "guardar otro canal no pisa el canal anterior")

# Borrar: guardar la grilla vacía borra ese nivel y solo ese.
upsert(pd.DataFrame(columns=dp.METAS_COLS), "2026-09", "FOOD", nivel="proveedor")
m = dp.cargar_metas(path=P)
chk(casi(dp.total_meta(m, "2026-09", "objetivo", "proveedor", "FOOD"), 0),
    "guardar la grilla vacía borra el nivel")
chk(casi(dp.total_meta(m, "2026-09", "objetivo", "canal", "FOOD"), 10000),
    "borrar un nivel no toca los otros")
upsert(prov, "2026-09", "FOOD", nivel="proveedor")   # se restaura

# Presupuesto: varios meses de una.
meses = [f"2026-{i:02d}" for i in range(1, 13)]
pres = pd.DataFrame([{"anio_mes": ms, "dsCanalMkt": "FOOD", "meta_kg": 9000}
                     for ms in meses])
dp.upsert_metas(pres, anio_mes=meses, canales=["FOOD"], tipo="presupuesto",
                nivel="canal", path=P, historial=False)
m = dp.cargar_metas(path=P)
chk(casi(dp.total_meta(m, "2026-09", "presupuesto", "canal", "FOOD"), 9000),
    "el presupuesto se guarda mes a mes")
chk(len(dp.filtrar_metas(m, tipo="presupuesto", nivel="canal", canal="FOOD")) == 12,
    "el presupuesto anual guarda los 12 meses en una sola escritura")
chk(casi(dp.total_meta(m, "2026-09", "objetivo", "canal", "FOOD"), 10000),
    "el presupuesto no pisa el objetivo del mismo mes/canal/nivel")


print("\n[4] Controles de consistencia")

v = dp.validar_metas(dp.cargar_metas(path=P), "2026-09", "FOOD")
por_control = dict(zip(v["control"], v["estado"]))
chk(por_control["Σ proveedores vs. objetivo del canal"] == "ok",
    "proveedores que cierran -> ok")
chk(por_control["Σ vendedores vs. objetivo del canal"] == "ok",
    "vendedores que cierran -> ok")
chk(por_control["Objetivo del mes vs. presupuesto anual"] == "info",
    "objetivo vs presupuesto es informativo, no error")
fila_pres = v[v["control"] == "Objetivo del mes vs. presupuesto anual"].iloc[0]
chk(casi(fila_pres["dif_kg"], 1000), "el desvío vs presupuesto se calcula bien")

# Desbalanceo deliberado.
upsert(pd.DataFrame([
    {"anio_mes": "2026-09", "dsCanalMkt": "FOOD", "marca_linea": "MCCAIN", "meta_kg": 6000},
    {"anio_mes": "2026-09", "dsCanalMkt": "FOOD", "marca_linea": "GARCIA", "meta_kg": 1000},
]), "2026-09", "FOOD", nivel="proveedor")
v = dp.validar_metas(dp.cargar_metas(path=P), "2026-09", "FOOD")
f = v[v["control"] == "Σ proveedores vs. objetivo del canal"].iloc[0]
chk(f["estado"] == "aviso", "apertura que no cierra -> aviso")
chk(casi(f["dif_kg"], -3000), "la diferencia se reporta con signo")
chk("de menos" in f["detalle"], "el detalle dice si falta o sobra")

# Tolerancia por redondeo.
upsert(pd.DataFrame([
    {"anio_mes": "2026-09", "dsCanalMkt": "FOOD", "marca_linea": "MCCAIN", "meta_kg": 6000},
    {"anio_mes": "2026-09", "dsCanalMkt": "FOOD", "marca_linea": "GARCIA", "meta_kg": 4010},
]), "2026-09", "FOOD", nivel="proveedor")
v = dp.validar_metas(dp.cargar_metas(path=P), "2026-09", "FOOD")
chk(v[v["control"] == "Σ proveedores vs. objetivo del canal"].iloc[0]["estado"] == "ok",
    "10 kg de diferencia sobre 10.000 entra en la tolerancia (0,5%)")

# Sin total de canal no hay contra qué validar.
v2 = dp.validar_metas(dp.cargar_metas(path=P), "2026-09", "GRANJAS")
chk(all(v2[v2["control"].str.startswith("Σ")]["estado"] == "falta"),
    "sin objetivo de canal los controles quedan en 'falta'")
chk(not v2.empty and "detalle" in v2.columns, "validar_metas siempre devuelve el detalle")

vacia = dp.validar_metas(dp.cargar_metas(path=P), "2026-09", "CANAL INEXISTENTE")
chk(len(vacia) >= 2, "un canal sin nada cargado igual devuelve los controles")


print("\n[5] Historial: objetivo original vs. vigente")

if os.path.exists(H):
    os.remove(H)
snap1 = dp.normalizar_metas(
    pd.DataFrame([{"anio_mes": "2026-09", "dsCanalMkt": "FOOD", "meta_kg": 10000}]),
    tipo="objetivo", nivel="canal")
snap1["fecha_carga"] = pd.Timestamp("2026-08-28 09:00:00")
dp._append_historial(snap1, path=H)
snap2 = dp.normalizar_metas(
    pd.DataFrame([{"anio_mes": "2026-09", "dsCanalMkt": "FOOD", "meta_kg": 8000}]),
    tipo="objetivo", nivel="canal")
snap2["fecha_carga"] = pd.Timestamp("2026-09-05 11:30:00")
dp._append_historial(snap2, path=H)

ov = dp.metas_original_vs_vigente("2026-09", "objetivo", "canal", "FOOD", path=H)
chk(ov is not None, "el historial devuelve el corte pedido")
chk(casi(ov["original_kg"], 10000), "guarda el objetivo original")
chk(casi(ov["vigente_kg"], 8000), "guarda el objetivo vigente tras el reajuste")
chk(ov["n_cargas"] == 2, "cuenta las cargas")
chk(dp.metas_original_vs_vigente("2026-01", "objetivo", "canal", "FOOD", path=H) is None,
    "sin historial devuelve None")


print("\n[6] Seguimiento por nivel")

ventas = pd.DataFrame([
    # canal, vendedor, marca, kilos, fecha
    ("FOOD", "AHMED", "MCCAIN", 2000, "2026-09-02"),
    ("FOOD", "AHMED", "GARCIA", 1000, "2026-09-03"),
    ("FOOD", "ROJAS", "MCCAIN", 1500, "2026-09-04"),
    ("GRANJAS", "LUGONES", "MCCAIN", 300, "2026-09-05"),
], columns=["dsCanalMkt", "dsVendedor", "marca_linea", "kilos", "fechaComprobate"])
ventas["fechaComprobate"] = pd.to_datetime(ventas["fechaComprobate"])
prev_ = ventas.copy()
prev_["kilos"] = prev_["kilos"] * 0.8
prev_["fechaComprobate"] = prev_["fechaComprobate"] - pd.DateOffset(months=1)

metas = dp.cargar_metas(path=P)

for niv, col in [("canal", None), ("proveedor", "marca_linea"),
                 ("vendedor", "dsVendedor")]:
    s = dp.seguimiento_metas(ventas, metas, "2026-09", dias_pasados=5,
                             dias_totales=25, df_mes_anterior=prev_,
                             canales=["FOOD"], nivel=niv)
    esperado = ["dsCanalMkt"] + ([col] if col else []) + [
        "meta_kg", "avance_kg", "proyeccion_kg", "alcance_pct", "brecha_kg",
        "falta_kg", "mes_ant_kg", "var_ant_pct"]
    chk(list(s.columns) == esperado, f"nivel '{niv}': columnas correctas")
    chk(casi(s["avance_kg"].sum(), 4500),
        f"nivel '{niv}': el avance total del canal es el mismo (4.500 kg)")

s_can = dp.seguimiento_metas(ventas, metas, "2026-09", dias_pasados=5,
                             dias_totales=25, df_mes_anterior=prev_,
                             canales=["FOOD"], nivel="canal")
chk(casi(s_can["proyeccion_kg"].iloc[0], 4500 / 5 * 25),
    "la proyección es avance ÷ días transcurridos × días del mes")
chk(casi(s_can["falta_kg"].iloc[0], 10000 - 4500), "falta_kg = meta - avance")
chk(casi(s_can["mes_ant_kg"].iloc[0], 3600), "trae los kilos del mes anterior")

s_cerr = dp.seguimiento_metas(ventas, metas, "2026-09", dias_pasados=25,
                              dias_totales=25, df_mes_anterior=prev_,
                              canales=["FOOD"], nivel="canal")
chk(casi(s_cerr["proyeccion_kg"].iloc[0], 4500),
    "mes cerrado: la proyección es el kilaje real")

s_v = dp.seguimiento_metas(ventas, metas, "2026-09", dias_pasados=5,
                           dias_totales=25, df_mes_anterior=prev_,
                           canales=["FOOD"], nivel="vendedor")
chk(set(s_v["dsVendedor"]) == {"AHMED", "ROJAS"},
    "el nivel vendedor trae los vendedores del canal")
chk(casi(float(s_v.loc[s_v["dsVendedor"] == "AHMED", "avance_kg"].iloc[0]), 3000),
    "suma los kilos del vendedor a través de sus marcas")

# Filas que vendieron sin meta cargada tienen que aparecer igual.
s_g = dp.seguimiento_metas(ventas, metas, "2026-09", dias_pasados=5,
                           dias_totales=25, df_mes_anterior=prev_,
                           canales=["GRANJAS"], nivel="vendedor")
chk(not s_g.empty and casi(s_g["meta_kg"].sum(), 0) and casi(s_g["avance_kg"].sum(), 300),
    "vendió sin meta cargada: aparece con meta 0, no se pierde volumen")
chk(bool(np.isnan(s_g["alcance_pct"].iloc[0])) and dp.semaforo(s_g["alcance_pct"].iloc[0]) == "⚪",
    "sin meta el alcance es NaN y el semáforo gris")

vacio_seg = dp.seguimiento_metas(pd.DataFrame(), metas, "2026-09",
                                 canales=["FOOD"], nivel="proveedor")
chk(isinstance(vacio_seg, pd.DataFrame), "seguimiento con ventas vacías no explota")


print("\n[7] Agregaciones de ventas")

kc = dp.kilos_por_canal(ventas)
chk(casi(kc.loc[kc["dsCanalMkt"] == "FOOD", "kilos"].iloc[0], 4500),
    "kilos_por_canal suma bien")
kv = dp.kilos_por_canal_vendedor(ventas)
chk(len(kv) == 3, "kilos_por_canal_vendedor agrupa por canal × vendedor")
km = dp.kilos_por_mes_canal(ventas)
chk(km["anio_mes"].unique().tolist() == ["2026-09"], "kilos_por_mes_canal arma el YYYY-MM")
chk(dp.kilos_por_canal(pd.DataFrame()).empty, "agregaciones con df vacío devuelven vacío")
chk(list(dp.kilos_por_canal_vendedor(pd.DataFrame()).columns) ==
    ["dsCanalMkt", "dsVendedor", "kilos"], "y conservan las columnas")


print("\n[9] Días de facturación de Food Service")

# Agosto 2026 arranca sábado. Corte al viernes 7.
_D, _C, _H = dt.date(2026, 8, 1), dt.date(2026, 8, 7), dt.date(2026, 8, 31)

chk(dp.dias_habiles(_D, _H) == 26 and dp.dias_habiles(_D, _C) == 6,
    "días hábiles de agosto 2026 (lunes a sábado)")
chk(dp.dias_habiles(_D, _H, feriados={dt.date(2026, 8, 17)}) == 25,
    "los feriados no cuentan como día hábil")

chk(dp.dias_facturacion_vendedor("FOOD SERVICE", "COLOMBO, CARLOS") == (0, 3),
    "lee los días declarados del vendedor")
chk(dp.dias_facturacion_vendedor("FOOD SERVICE", "  colombo,  carlos ") == (0, 3),
    "el nombre se compara normalizado (mayúsculas y espacios)")
chk(dp.dias_facturacion_vendedor("GRANJAS", "MORENO GERMAN") is None,
    "fuera de Food Service no aplica: MORENO vende también en GRANJAS")
chk(dp.dias_facturacion_vendedor("FOOD SERVICE", "FOOD CABA") is None,
    "vendedor sin días declarados devuelve None (cae a días hábiles)")

# Colombo factura lunes y jueves: en agosto son 9, y al 7 pasaron 2 (3 y 6).
chk(dp.contar_dias_facturacion(_D, _H, (0, 3)) == 9,
    "cuenta los lunes y jueves del mes")
chk(dp.dias_venta("FOOD SERVICE", "COLOMBO, CARLOS", _D, _C, _H) == (2, 9),
    "días de venta de un vendedor de Food: sus días de facturación")
chk(dp.dias_venta("RETAIL", "AHMED, GISELA", _D, _C, _H) == (6, 26),
    "un canal que factura todos los días sigue con días hábiles")
chk(casi(dp.factor_proyeccion("FOOD SERVICE", "COLOMBO, CARLOS", _D, _C, _H),
         9 / 2, tol=0.001),
    "el factor de proyección es días totales ÷ transcurridos")
chk(dp.factor_proyeccion("FOOD SERVICE", "COLOMBO, CARLOS", _D, _H, _H) == 1.0,
    "mes cerrado: factor 1, la proyección es el kilaje real")
chk(dp.etiqueta_dias_facturacion("AVETTA SANCHEZ, MARIA NOELIA")
    == "lunes, miércoles y viernes",
    "etiqueta legible de los días")

# Ventas de agosto: dos vendedores de Food con días distintos + uno de Retail.
vf = pd.DataFrame({
    "fechaComprobate": pd.to_datetime(["2026-08-03"] * 3),
    "dsCanalMkt": ["FOOD SERVICE", "FOOD SERVICE", "RETAIL"],
    "dsVendedor": ["COLOMBO, CARLOS", "AVETTA SANCHEZ, MARIA NOELIA",
                   "AHMED, GISELA"],
    "marca_linea": ["MCCAIN FOOD", "MCCAIN FOOD", "MCCAIN RETAIL"],
    "kilos": [100.0, 100.0, 100.0],
})

_esperado = 100 * (9 / 2) + 100 * (13 / 3) + 100 * (26 / 6)

s_can = dp.seguimiento_metas(vf, dp._metas_vacio(), "2026-08", nivel="canal",
                             desde=_D, corte=_C, hasta=_H)
s_prov = dp.seguimiento_metas(vf, dp._metas_vacio(), "2026-08", nivel="proveedor",
                              desde=_D, corte=_C, hasta=_H)
s_vend = dp.seguimiento_metas(vf, dp._metas_vacio(), "2026-08", nivel="vendedor",
                              desde=_D, corte=_C, hasta=_H)

chk(casi(s_can["proyeccion_kg"].sum(), _esperado, tol=0.01),
    "la proyección se calcula con los días de cada vendedor, no con uno global")
chk(casi(s_can["proyeccion_kg"].sum(), s_prov["proyeccion_kg"].sum(), tol=0.01)
    and casi(s_can["proyeccion_kg"].sum(), s_vend["proyeccion_kg"].sum(), tol=0.01),
    "canal = Σ proveedores = Σ vendedores (los tres niveles cierran)")

_food = s_can[s_can["dsCanalMkt"] == "FOOD SERVICE"]["proyeccion_kg"].iloc[0]
chk(casi(_food, 100 * (9 / 2) + 100 * (13 / 3), tol=0.01),
    "Food Service proyecta contra días de facturación")
_retail = s_can[s_can["dsCanalMkt"] == "RETAIL"]["proyeccion_kg"].iloc[0]
chk(casi(_retail, 100 * (26 / 6), tol=0.01),
    "Retail sigue proyectando contra días hábiles: no cambió nada")

# Sin fechas se mantiene el modo viejo (compatibilidad hacia atrás).
s_viejo = dp.seguimiento_metas(vf, dp._metas_vacio(), "2026-08", nivel="canal",
                               dias_pasados=6, dias_totales=26)
chk(casi(s_viejo["proyeccion_kg"].sum(), 300 * 26 / 6, tol=0.01),
    "sin fechas usa el factor global de siempre (firma vieja intacta)")

# Vendedor sin días declarados: proyecta como antes, y queda listado.
vsd = pd.DataFrame({
    "dsCanalMkt": ["FOOD SERVICE", "FOOD SERVICE", "RETAIL"],
    "dsVendedor": ["FOOD CABA", "COLOMBO, CARLOS", "AHMED, GISELA"],
    "marca_linea": ["MCCAIN FOOD"] * 3,
    "kilos": [100.0, 100.0, 100.0],
})
chk(dp.vendedores_sin_dias_facturacion(vsd) == ["FOOD CABA"],
    "lista los vendedores de Food sin días declarados (avisa, no bloquea)")
s_sd = dp.seguimiento_metas(vsd, dp._metas_vacio(), "2026-08", nivel="vendedor",
                            desde=_D, corte=_C, hasta=_H)
_caba = s_sd[s_sd["dsVendedor"] == "FOOD CABA"]["proyeccion_kg"].iloc[0]
chk(casi(_caba, 100 * (26 / 6), tol=0.01),
    "el vendedor sin días declarados cae a días hábiles (criterio viejo)")

# Resumen de días para los textos del tablero.
_pas, _tot, _mixto = dp.dias_venta_resumen(vf, _D, _C, _H)
chk(_mixto is True, "avisa que la vista mezcla días de facturación")
_pr, _tr, _mr = dp.dias_venta_resumen(vf, _D, _C, _H, canales=["RETAIL"])
chk((_pr, _tr, _mr) == (6, 26, False),
    "un canal sin días de facturación devuelve exactamente días hábiles")

# Factor ponderado (el que usa el tab Resumen).
_f, _p = dp.factor_proyeccion_ponderado(vf, _D, _C, _H)
chk(_p and casi(_f, _esperado / 300, tol=0.001),
    "el factor ponderado equivale a la suma de proyecciones por vendedor")
_f2, _p2 = dp.factor_proyeccion_ponderado(vf, _D, _H, _H)
chk(_p2 is False and _f2 == 1.0,
    "mes cerrado: el tab Resumen no proyecta")
chk(dp.factor_proyeccion_ponderado(pd.DataFrame(), _D, _C, _H) == (1.0, False),
    "sin ventas no proyecta y no explota")


print("\n[8] Íconos y semáforo")

chk(dp.semaforo(120) == "🟢" and dp.semaforo(95) == "🟡" and dp.semaforo(50) == "🔴",
    "semáforo por alcance proyectado")
chk(dp.icono_validacion("ok") == "✅" and dp.icono_validacion("aviso") == "⚠️",
    "íconos de validación")


print("\n" + "=" * 60)
if FALLOS:
    print(f"{OK} ok · {len(FALLOS)} FALLAS")
    for f in FALLOS:
        print("  -", f)
    sys.exit(1)
print(f"{OK} checks OK")
