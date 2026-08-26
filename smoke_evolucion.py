"""Smoke test de la EVOLUCIÓN MENSUAL (serie histórica + filtros globales).

El gráfico de evolución no se arma con `df` (el detalle del período) sino con
data/serie_mensual.parquet, que está pre-agregada. Por eso los filtros de la
barra de arriba tienen que aplicarse a mano sobre la serie: si una dimensión no
está en dp.SERIE_GRANO, el filtro NO puede aplicarse y hay que avisarlo en vez
de mostrar un gráfico sin filtrar (que es lo que hacía antes con Región,
Marca/Línea y Cliente).

Chequea:
  0. dp.SERIE_GRANO contiene las dimensiones filtrables que debe contener.
  1. dp.agregar_serie produce todas las columnas del grano, derivando 'region'
     y 'marca_linea' si el detalle no las trae.
  2. dp.filtrar_serie: filtra por cada dimensión, combina varias, avisa las que
     no puede aplicar (Cliente) y corta por mes (el filtro de Período).
  3. Invariante clave: filtrar NUNCA puede agrandar el total, y filtrar por
     todos los valores de una dimensión tiene que dar el total original.
  4. dp.alinear_serie: una serie vieja (sin las dimensiones nuevas) se lee sin
     romper y sus filas quedan marcadas como SIN CLASIFICAR.
  5. Los bordes: serie vacía, selección vacía, columna inexistente, mes fuera
     de rango. Nada de esto puede tirar una excepción.

No escribe nada: es de solo lectura (la serie real solo se lee si existe).

Uso:  python3 smoke_evolucion.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import numpy as np                         # noqa: E402
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


print("\nSmoke test · Evolución mensual")

# --- 0. El grano de la serie ------------------------------------------------
print("\n[0] Grano de la serie")
for col in ["anio_mes", "dsCanalMkt", "dsSubcanalMKT", "dsVendedor",
            "region", "marca_linea"]:
    chk(col in dp.SERIE_GRANO, f"SERIE_GRANO incluye '{col}'")
chk("nombreCliente" not in dp.SERIE_GRANO,
    "SERIE_GRANO NO incluye 'nombreCliente' (a propósito: son ~900 por mes)")


# --- 1. agregar_serie deriva las dimensiones que faltan ---------------------
print("\n[1] agregar_serie sobre un detalle sintético")

rng = np.random.default_rng(7)
N = 400
detalle = pd.DataFrame({
    "fechaComprobate": pd.to_datetime("2026-06-01")
    + pd.to_timedelta(rng.integers(0, 55, N), unit="D"),
    "dsCanalMkt": rng.choice(["MAYORISTA", "FOOD SERVICE"], N),
    "dsSubcanalMKT": rng.choice(["ALMACEN", "RESTO"], N),
    "dsVendedor": rng.choice(["VEND A", "VEND B", "VEND C"], N),
    "dsLocalidad": rng.choice(["SAN MIGUEL", "DEL VISO", "LOCALIDAD RARA"], N),
    "idArticulo": rng.integers(100, 140, N),
    "proveedor": rng.choice(["9 - MC CAIN SA", "3 - ELCOR"], N),
    "idCliente": rng.integers(1, 40, N),
    "nombreCliente": rng.choice(["CLIENTE 1", "CLIENTE 2"], N),
    "kilos": rng.random(N) * 10,
    "subtotalNeto": rng.random(N) * 1000,
    "costo_unitario": rng.random(N) * 500,
    "dsEmpresa": "GAVEN",
    "dsDocumento": "FACTURA",
    "nrodoc": rng.integers(1, 120, N),
})

serie = dp.agregar_serie(detalle)
chk(list(serie.columns) == dp.SERIE_COLS,
    "agregar_serie devuelve exactamente SERIE_COLS",
    f"{list(serie.columns)}")
chk(serie["region"].notna().all() and (serie["region"] != "").all(),
    "'region' se deriva de dsLocalidad aunque el detalle no la traiga",
    f"regiones: {sorted(serie['region'].unique())}")
chk(serie["marca_linea"].nunique() >= 2,
    "'marca_linea' se deriva por lookup de artículo",
    f"marcas: {sorted(serie['marca_linea'].unique())[:6]}")
chk(abs(serie["subtotalNeto"].sum() - detalle["subtotalNeto"].sum()) < 1e-6,
    "agregar no pierde ni inventa facturación (suma == detalle)")

# Si el detalle YA trae las columnas, se respetan (no se recalculan).
det2 = detalle.copy()
det2["region"] = "REGION PROPIA"
det2["marca_linea"] = "MARCA PROPIA"
s2 = dp.agregar_serie(det2)
chk(set(s2["region"]) == {"REGION PROPIA"}
    and set(s2["marca_linea"]) == {"MARCA PROPIA"},
    "si el detalle ya trae region/marca_linea, agregar_serie las respeta")


# --- 2. filtrar_serie -------------------------------------------------------
print("\n[2] filtrar_serie (los filtros de la barra de arriba)")

total_fc = serie["subtotalNeto"].sum()

f, sin_aplicar = dp.filtrar_serie(serie, {})
chk(len(f) == len(serie) and not sin_aplicar,
    "sin filtros no filtra nada y no avisa nada")

f, sin_aplicar = dp.filtrar_serie(serie, {"dsCanalMkt": []})
chk(len(f) == len(serie), "un filtro vacío ([]) no filtra")

for col, valor in [("dsCanalMkt", "MAYORISTA"),
                   ("dsSubcanalMKT", "ALMACEN"),
                   ("dsVendedor", "VEND A"),
                   ("region", "SAN MIGUEL"),
                   ("marca_linea", sorted(serie["marca_linea"].unique())[0])]:
    f, sin_aplicar = dp.filtrar_serie(serie, {col: [valor]})
    esperado = serie[serie[col] == valor]["subtotalNeto"].sum()
    chk(not f.empty and abs(f["subtotalNeto"].sum() - esperado) < 1e-6
        and not sin_aplicar,
        f"filtra por {col} = '{valor}'",
        f"{len(f)} filas · $ {f['subtotalNeto'].sum():,.0f}")

# Varios filtros a la vez = intersección.
f, _ = dp.filtrar_serie(serie, {"dsCanalMkt": ["MAYORISTA"],
                                "region": ["SAN MIGUEL"]})
esperado = serie[(serie["dsCanalMkt"] == "MAYORISTA")
                 & (serie["region"] == "SAN MIGUEL")]["subtotalNeto"].sum()
chk(abs(f["subtotalNeto"].sum() - esperado) < 1e-6,
    "dos filtros combinados dan la intersección, no la unión")

# Cliente NO está en el grano: no se aplica, pero se avisa.
f, sin_aplicar = dp.filtrar_serie(serie, {"nombreCliente": ["CLIENTE 1"]})
chk(sin_aplicar == ["nombreCliente"],
    "el filtro de Cliente se devuelve en `sin_aplicar` (la app lo avisa)")
chk(abs(f["subtotalNeto"].sum() - total_fc) < 1e-6,
    "y no altera los números (no se aplica a medias)")

# Período: corta hasta el mes elegido, inclusive.
meses = sorted(serie["anio_mes"].unique())
chk(len(meses) >= 2, "el detalle sintético cubre más de un mes", f"{meses}")
f, _ = dp.filtrar_serie(serie, {}, hasta_mes=meses[0])
chk(set(f["anio_mes"]) == {meses[0]},
    f"hasta_mes='{meses[0]}' deja solo ese mes (es el primero)")
f, _ = dp.filtrar_serie(serie, {}, hasta_mes=meses[-1])
chk(len(f) == len(serie),
    f"hasta_mes='{meses[-1]}' (el último) no recorta nada")
f, _ = dp.filtrar_serie(serie, {}, hasta_mes="1999-01")
chk(f.empty, "un mes anterior a toda la serie devuelve vacío, no rompe")
f, _ = dp.filtrar_serie(serie, {}, hasta_mes="2099-12")
chk(len(f) == len(serie), "un mes posterior a toda la serie no recorta")


# --- 3. Invariantes ---------------------------------------------------------
print("\n[3] Invariantes")

for col in ["dsCanalMkt", "region", "marca_linea"]:
    todos = sorted(serie[col].unique())
    f, _ = dp.filtrar_serie(serie, {col: todos})
    chk(abs(f["subtotalNeto"].sum() - total_fc) < 1e-6,
        f"filtrar por TODOS los valores de {col} da el total original")
    f, _ = dp.filtrar_serie(serie, {col: todos[:1]})
    chk(f["subtotalNeto"].sum() <= total_fc + 1e-6,
        f"filtrar por {col} nunca agranda el total")

f, _ = dp.filtrar_serie(serie, {"dsCanalMkt": ["CANAL QUE NO EXISTE"]})
chk(f.empty, "un valor inexistente devuelve vacío (la app muestra el cartel)")

# La serie original no se toca (la app la tiene cacheada con @st.cache_data:
# mutarla ensuciaría el caché para todas las corridas siguientes).
antes = serie["subtotalNeto"].sum()
_copia = dp.filtrar_serie(serie, {"dsCanalMkt": ["MAYORISTA"]})[0]
_copia.loc[:, "subtotalNeto"] = 0
chk(abs(serie["subtotalNeto"].sum() - antes) < 1e-6,
    "filtrar_serie no modifica la serie original (importa: está cacheada)")


# --- 4. Series viejas (sin las dimensiones nuevas) --------------------------
print("\n[4] Compatibilidad con la serie vieja")

vieja = serie.drop(columns=["region", "marca_linea"])
alineada, faltantes = dp.alinear_serie(vieja)
chk(sorted(faltantes) == ["marca_linea", "region"],
    "alinear_serie detecta las dimensiones que faltan")
chk(all(c in alineada.columns for c in dp.SERIE_GRANO),
    "y deja la serie con todas las columnas del grano")
chk(set(alineada["region"]) == {dp.SERIE_SIN_DATO},
    f"las filas viejas quedan como '{dp.SERIE_SIN_DATO}'")
f, _ = dp.filtrar_serie(alineada, {"region": ["SAN MIGUEL"]})
chk(f.empty,
    "y quedan FUERA al filtrar por esa dimensión (no sabemos a cuál van)")
f, _ = dp.filtrar_serie(alineada, {"dsCanalMkt": ["MAYORISTA"]})
chk(not f.empty,
    "pero siguen respondiendo a los filtros que sí existían")

ya_ok, faltantes = dp.alinear_serie(serie)
chk(not faltantes and len(ya_ok) == len(serie),
    "una serie al día pasa por alinear_serie sin cambios")


# --- 5. Bordes --------------------------------------------------------------
print("\n[5] Bordes (nada de esto puede tirar excepción)")

vacia = pd.DataFrame(columns=dp.SERIE_COLS)
try:
    f, sa = dp.filtrar_serie(vacia, {"dsCanalMkt": ["X"]}, hasta_mes="2026-06")
    chk(f.empty, "serie vacía + filtros: devuelve vacío")
except Exception as e:  # noqa: BLE001
    chk(False, "serie vacía + filtros", f"{type(e).__name__}: {e}")

try:
    f, sa = dp.filtrar_serie(serie, None)
    chk(len(f) == len(serie) and sa == [], "seleccion=None no rompe")
except Exception as e:  # noqa: BLE001
    chk(False, "seleccion=None", f"{type(e).__name__}: {e}")

try:
    f, sa = dp.filtrar_serie(serie, {"columna_inventada": ["x"]})
    chk(sa == ["columna_inventada"],
        "una columna que no existe se avisa, no rompe")
except Exception as e:  # noqa: BLE001
    chk(False, "columna inexistente", f"{type(e).__name__}: {e}")

try:
    chk(dp.agregar_serie(pd.DataFrame()).empty,
        "agregar_serie con un df vacío devuelve vacío")
    chk(dp.alinear_serie(pd.DataFrame())[0].empty,
        "alinear_serie con un df vacío devuelve vacío")
except Exception as e:  # noqa: BLE001
    chk(False, "df vacío", f"{type(e).__name__}: {e}")


# --- 6. La serie real (si existe) ------------------------------------------
print("\n[6] Serie real en disco")

if not os.path.exists(dp.SERIE_PATH):
    print("  --    no hay data/serie_mensual.parquet todavía (se saltea)")
else:
    real = pd.read_parquet(dp.SERIE_PATH)
    real, faltantes = dp.alinear_serie(real)
    if faltantes:
        print(f"  aviso la serie guardada no tiene {faltantes}: esos meses "
              f"quedan en '{dp.SERIE_SIN_DATO}'.")
        print("         Para recuperarlos: python backfill_serie.py --reset")
    chk(all(c in real.columns for c in dp.SERIE_COLS),
        "la serie real se lee con todas las columnas del grano actual")
    meses = sorted(real["anio_mes"].unique())
    ult = meses[-1]
    f, _ = dp.filtrar_serie(real, {}, hasta_mes=ult)
    chk(len(f) == len(real),
        f"cortar en el último mes ({ult}) no recorta nada",
        f"{len(meses)} meses: {meses[0]} → {ult}")
    if len(meses) > 1:
        f, _ = dp.filtrar_serie(real, {}, hasta_mes=meses[-2])
        chk(f["anio_mes"].max() == meses[-2],
            f"cortar en {meses[-2]} deja fuera {ult}")
    canales = sorted(real["dsCanalMkt"].dropna().unique())
    if canales:
        f, _ = dp.filtrar_serie(real, {"dsCanalMkt": canales[:1]})
        chk(0 < f["subtotalNeto"].sum() <= real["subtotalNeto"].sum() + 1e-6,
            f"filtrar la serie real por canal '{canales[0]}' da una parte "
            "del total")


print(f"\n{'TODO OK' if not fallas else str(fallas) + ' FALLA(S)'}\n")
sys.exit(1 if fallas else 0)
