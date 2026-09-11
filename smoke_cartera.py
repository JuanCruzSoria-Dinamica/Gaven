"""Smoke test de las MÉTRICAS DE CARTERA del evolutivo y de la PROYECCIÓN del
mes en curso.

Las tres métricas nuevas (clientes activos, SKUs distintos, SKUs por cliente)
son conteos únicos, no sumas. Por eso NO se pueden guardar como número en la
serie mensual: un cliente que le compra a dos vendedores aparece en dos filas
del grano y sumarlas lo cuenta dos veces. Se guardan las combinaciones
(data/serie_cartera.parquet) y se cuenta al leer, después de filtrar.

Chequea:
  1. dp.agregar_cartera: grano completo, filas únicas, deriva las dimensiones
     que el detalle no traiga, ignora cliente/artículo vacíos.
  2. dp.metricas_cartera: los tres números, con y sin apertura por dimensión.
  3. LA INVARIANTE QUE JUSTIFICA TODO ESTO: el total contado es MENOR O IGUAL
     que la suma de las partes, y estrictamente menor cuando hay un cliente
     compartido entre dos vendedores. Si esto se rompiera, alcanzaría con
     guardar conteos en la serie.
  4. Filtrar la cartera con dp.filtrar_serie (la misma función que la serie) da
     los mismos resultados que filtrar el detalle a mano.
  5. dp.proyectar_serie_mes: escala SOLO las columnas que se acumulan, deja los
     conteos intactos, respeta los días de facturación de Food Service y no
     proyecta un mes cerrado.
  6. Bordes: vacíos, columnas faltantes, un solo día de venta.

No escribe nada fuera de un archivo temporal propio.

Uso:  python3 smoke_cartera.py
"""
import os
import sys
import shutil
import tempfile
import datetime as dt

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


def det(filas):
    """Detalle mínimo: (fecha, cliente, articulo, canal, vendedor, kilos)."""
    d = pd.DataFrame(filas, columns=["fechaComprobate", "idCliente",
                                     "idArticulo", "dsCanalMkt", "dsVendedor",
                                     "kilos"])
    d["fechaComprobate"] = pd.to_datetime(d["fechaComprobate"])
    d["dsSubcanalMKT"] = "SUB"
    d["region"] = "NEA"
    d["marca_linea"] = "MARCA"
    d["subtotalNeto"] = d["kilos"] * 10
    d["costo_unitario"] = d["kilos"] * 6
    # Las necesita agregar_serie() para contar comprobantes.
    d["dsEmpresa"] = "GAVEN"
    d["dsDocumento"] = "FA"
    d["nrodoc"] = range(1, len(d) + 1)
    return d


print("\nSmoke test · Cartera del evolutivo (clientes, SKUs, SKUs por cliente)")

# --- 1. agregar_cartera -----------------------------------------------------
print("\n[1] dp.agregar_cartera")
base = det([
    ("2026-08-03", "C1", "A1", "RETAIL", "V1", 10),
    ("2026-08-03", "C1", "A1", "RETAIL", "V1", 5),   # repetida: no duplica
    ("2026-08-04", "C1", "A2", "RETAIL", "V1", 7),
    ("2026-08-04", "C2", "A1", "RETAIL", "V2", 3),
])
c = dp.agregar_cartera(base)
chk(list(c.columns) == dp.CARTERA_GRANO,
    "devuelve exactamente el grano CARTERA_GRANO", str(list(c.columns)))
chk(len(c) == 3, "3 combinaciones únicas de 4 filas de detalle", f"{len(c)}")
chk(not c.duplicated().any(), "no quedan filas duplicadas")

sin_dims = base.drop(columns=["region", "marca_linea"])
c2 = dp.agregar_cartera(sin_dims)
chk(list(c2.columns) == dp.CARTERA_GRANO,
    "un detalle sin 'region'/'marca_linea' igual sale con el grano completo")

vacios = det([("2026-08-03", "", "A1", "RETAIL", "V1", 1),
              ("2026-08-03", "C9", "nan", "RETAIL", "V1", 1)])
chk(dp.agregar_cartera(vacios).empty,
    "cliente o artículo vacío no entra (ensuciaría el promedio)")

# --- 2. metricas_cartera ----------------------------------------------------
print("\n[2] dp.metricas_cartera")
m = dp.metricas_cartera(c)
chk(len(m) == 1 and m.loc[0, "clientes"] == 2, "clientes activos = 2",
    f"{m.to_dict('records')}")
chk(m.loc[0, "skus"] == 2, "SKUs distintos = 2")
chk(abs(m.loc[0, "sku_por_cliente"] - 1.5) < 1e-9,
    "SKUs por cliente = 3 pares / 2 clientes = 1,5")

mv = dp.metricas_cartera(c, "dsVendedor")
chk(len(mv) == 2, "abierto por vendedor: una fila por vendedor")
chk(set(mv.columns) == {"anio_mes", "dsVendedor", "clientes", "skus",
                        "sku_por_cliente"},
    "columnas de salida con apertura")

# --- 3. La invariante: contar != sumar --------------------------------------
print("\n[3] El total se CUENTA, no se suma (es el motivo de esta tabla)")
compartido = det([
    ("2026-08-03", "C1", "A1", "RETAIL", "V1", 10),   # C1 le compra a V1
    ("2026-08-05", "C1", "A1", "RETAIL", "V2", 10),   # ...y también a V2
    ("2026-08-05", "C2", "A2", "RETAIL", "V2", 10),
])
cc = dp.agregar_cartera(compartido)
tot = dp.metricas_cartera(cc)
partes = dp.metricas_cartera(cc, "dsVendedor")
chk(tot.loc[0, "clientes"] == 2,
    "total contado = 2 clientes (C1 no se cuenta dos veces)")
chk(partes["clientes"].sum() == 3,
    "sumar las partes daría 3: por eso no se puede guardar el conteo")
chk(tot.loc[0, "clientes"] < partes["clientes"].sum(),
    "total < suma de partes cuando hay un cliente compartido")
chk(tot.loc[0, "skus"] == 2 and tot.loc[0, "sku_por_cliente"] == 1.0,
    "SKUs = 2 y SKUs por cliente = 1,0 (2 pares únicos / 2 clientes): el par "
    "(C1,A1) tampoco se cuenta dos veces por venir de dos vendedores",
    f"{tot.to_dict('records')}")

# --- 4. Filtrar la cartera con la misma función que la serie ----------------
print("\n[4] dp.filtrar_serie sobre la cartera")
f, sin_aplicar = dp.filtrar_serie(cc, {"dsVendedor": ["V2"]})
mf = dp.metricas_cartera(f)
chk(mf.loc[0, "clientes"] == 2 and mf.loc[0, "skus"] == 2,
    "filtrando por V2: 2 clientes, 2 SKUs (contado sobre lo filtrado)")
f2, sin_aplicar2 = dp.filtrar_serie(cc, {"nombreCliente": ["X"]})
chk(sin_aplicar2 == ["nombreCliente"],
    "el filtro de Cliente se avisa como no aplicable, igual que en la serie")
chk(dp.metricas_cartera(dp.filtrar_serie(cc, {}, hasta_mes="2026-07")[0]).empty,
    "cortar por período deja la cartera vacía si el mes no entra")

# --- 5. Proyección del mes en curso ----------------------------------------
print("\n[5] dp.proyectar_serie_mes")
serie = dp.agregar_serie(det([
    ("2026-08-03", "C1", "A1", "RETAIL", "V1", 100),
    ("2026-08-03", "C2", "A1", "FOOD SERVICE", "COLOMBO, CARLOS", 100),
]))
ini, corte, fin = dt.date(2026, 8, 1), dt.date(2026, 8, 12), dt.date(2026, 8, 31)
p, hubo = dp.proyectar_serie_mes(serie, ini, corte, fin)
chk(hubo, "con el mes a medio andar, proyecta")
for col in dp.SERIE_COLS_PROYECTABLES:
    chk((p[col] >= serie[col]).all(), f"'{col}' proyectado >= real")
chk((p["clientes"] == serie["clientes"]).all(),
    "'clientes' NO se toca: un cliente que compró tres veces sigue siendo uno")
chk((p["comprobantes"] == serie["comprobantes"]).all(),
    "'comprobantes' tampoco se escala")

_food = p[p["dsCanalMkt"] == "FOOD SERVICE"]
_resto = p[p["dsCanalMkt"] == "RETAIL"]
f_food = float(_food["kilos"].iloc[0]) / 100
f_resto = float(_resto["kilos"].iloc[0]) / 100
chk(abs(f_food - dp.factor_proyeccion("FOOD SERVICE", "COLOMBO, CARLOS",
                                      ini, corte, fin)) < 1e-9,
    f"Food Service usa los días de facturación del vendedor (×{f_food:.2f})")
chk(abs(f_resto - dp.factor_proyeccion("RETAIL", "V1", ini, corte, fin)) < 1e-9,
    f"el resto de los canales usa días hábiles (×{f_resto:.2f})")
chk(f_food != f_resto, "los dos factores son distintos (no hay factor único)")

_, cerrado = dp.proyectar_serie_mes(serie, ini, fin, fin)
chk(not cerrado, "un mes cerrado no se proyecta (devuelve proyecto=False)")

# --- 6. Bordes --------------------------------------------------------------
print("\n[6] Bordes (nada de esto puede tirar excepción)")
chk(dp.agregar_cartera(pd.DataFrame()).empty, "agregar_cartera con df vacío")
chk(dp.metricas_cartera(pd.DataFrame()).empty, "metricas_cartera con df vacío")
chk(dp.metricas_cartera(c, "columna_que_no_existe").empty,
    "metricas_cartera con una dimensión inexistente devuelve vacío, no rompe")
chk(dp.alinear_cartera(pd.DataFrame())[0].empty, "alinear_cartera con df vacío")
vieja = c.drop(columns=["region", "marca_linea"])
al, falt = dp.alinear_cartera(vieja)
chk(set(falt) == {"region", "marca_linea"}
    and (al["region"] == dp.SERIE_SIN_DATO).all(),
    "una cartera vieja se rellena con SIN CLASIFICAR y avisa qué faltaba")
_, sin_nada = dp.proyectar_serie_mes(pd.DataFrame(), ini, corte, fin)
chk(not sin_nada, "proyectar_serie_mes con df vacío no rompe")

# --- 7. Ida y vuelta por disco (upsert idempotente) -------------------------
print("\n[7] upsert_cartera (atómico e idempotente)")
tmpdir = tempfile.mkdtemp()
try:
    ruta = os.path.join(tmpdir, "serie_cartera.parquet")
    dp.upsert_cartera(base, cartera_path=ruta)
    n1 = len(pd.read_parquet(ruta))
    dp.upsert_cartera(base, cartera_path=ruta)
    n2 = len(pd.read_parquet(ruta))
    chk(n1 == n2 == 3, "correrlo dos veces con el mismo mes no duplica filas",
        f"{n1} → {n2}")
    otro = det([("2026-09-02", "C3", "A3", "RETAIL", "V1", 5)])
    dp.upsert_cartera(otro, cartera_path=ruta)
    leida = pd.read_parquet(ruta)
    chk(set(leida["anio_mes"]) == {"2026-08", "2026-09"},
        "un mes nuevo se agrega sin pisar el anterior")
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)

# --- 8. Datos reales --------------------------------------------------------
print("\n[8] Detalle real en disco")
if os.path.exists(dp.PARQUET_PATH):
    real = pd.read_parquet(dp.PARQUET_PATH)
    cr = dp.agregar_cartera(real)
    mr = dp.metricas_cartera(cr)
    pr = dp.metricas_cartera(cr, "dsCanalMkt")
    chk(not cr.empty, f"cartera real: {len(cr)} filas, "
        f"{cr['anio_mes'].nunique()} meses")
    sumas = pr.groupby("anio_mes")["clientes"].sum()
    totales = mr.set_index("anio_mes")["clientes"]
    chk((totales <= sumas.reindex(totales.index)).all(),
        "en los datos reales el total contado nunca supera la suma por canal")
    chk((mr["clientes"] > 0).all() and (mr["skus"] > 0).all(),
        "todos los meses tienen clientes y SKUs > 0")
    chk((mr["sku_por_cliente"] >= 1).all(),
        "SKUs por cliente >= 1 en todos los meses (quien compra, compra algo)")
    print("         " + mr.tail(3).to_string(index=False).replace("\n", "\n         "))
else:
    print("  (sin parquet de detalle: se omite)")

print("\n" + ("TODO OK" if not fallas else f"{fallas} FALLA(S)"))
sys.exit(1 if fallas else 0)
