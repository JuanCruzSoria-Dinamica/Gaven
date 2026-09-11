"""Smoke test del renombre de vendedores (dp.MAPA_VENDEDORES).

Chess trae algunos vendedores con un código GENÉRICO de operación ("FOOD CABA",
"RETAIL VENTAS") que en realidad es de una persona que además tiene su propio
código. El panel los mostraba como dos líneas distintas. dp.normalizar_vendedor
traduce el nombre genérico al real, con lo cual los dos códigos se fusionan en
una sola persona.

Se aplica en tres lugares y este test cubre los tres, porque si falta uno el
renombre se ve a medias: en preparar() (detalle que baja del API), al leer la
serie (alinear_serie) y al leer la cartera (alinear_cartera). La app además lo
aplica al leer el parquet guardado, que trae el nombre con el que bajó cada mes.

Chequea:
  1. El renombre en sí: traduce, no toca a los demás, no pierde filas ni kilos.
  2. Que fusione: los dos códigos quedan bajo un mismo nombre y el idVendedor
     original NO se toca (el dato crudo sigue siendo rastreable).
  3. Que la comparación sea tolerante (minúsculas, espacios de más).
  4. Que la serie y la cartera guardadas con el nombre viejo se traduzcan al
     leerlas, sin tener que rehacer el backfill.
  5. La consecuencia a vigilar: al fusionarse, Food Caba hereda los días de
     facturación declarados de Castillón.
  6. Bordes: df vacío, columna ausente, mapa que no aplica a nada.

Uso:  python3 smoke_vendedores.py
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


print("\nSmoke test · Renombre de vendedores")

print("\n[1] El mapa está cargado")
chk(dp.MAPA_VENDEDORES.get("RETAIL VENTAS") == "SANABRIA, GONZALO",
    "RETAIL VENTAS -> SANABRIA, GONZALO")
chk(dp.MAPA_VENDEDORES.get("FOOD CABA") == "CASTILLON AGUSTIN DAMIAN",
    "FOOD CABA -> CASTILLON AGUSTIN DAMIAN")
chk(all(v not in dp.MAPA_VENDEDORES for v in dp.MAPA_VENDEDORES.values()),
    "ningún destino es a su vez origen (no hay renombres encadenados)")

print("\n[2] normalizar_vendedor")
d = pd.DataFrame({
    "idVendedor": [36, 37, 75, 35, 13],
    "dsVendedor": ["RETAIL VENTAS", "SANABRIA, GONZALO", "FOOD CABA",
                   "CASTILLON AGUSTIN DAMIAN", "COLOMBO, CARLOS"],
    "kilos": [10.0, 20.0, 30.0, 40.0, 50.0],
})
n = dp.normalizar_vendedor(d)
chk(set(n["dsVendedor"]) == {"SANABRIA, GONZALO", "CASTILLON AGUSTIN DAMIAN",
                             "COLOMBO, CARLOS"},
    "quedan 3 vendedores de 5 nombres (los genéricos se fusionan)",
    str(sorted(set(n["dsVendedor"]))))
chk((n["dsVendedor"] == "COLOMBO, CARLOS").sum() == 1,
    "el resto de los vendedores no se toca")
chk(len(n) == len(d) and n["kilos"].sum() == d["kilos"].sum(),
    "no pierde filas ni kilos")
chk(list(n["idVendedor"]) == list(d["idVendedor"]),
    "idVendedor queda como viene de Chess (el dato crudo sigue rastreable)")
g = n.groupby("dsVendedor")["kilos"].sum()
chk(g["SANABRIA, GONZALO"] == 30 and g["CASTILLON AGUSTIN DAMIAN"] == 70,
    "los kilos de los dos códigos se suman en una sola línea")
chk(dp.normalizar_vendedor(d) is not d, "no modifica el df original")
chk(list(d["dsVendedor"])[0] == "RETAIL VENTAS", "el original queda intacto")

print("\n[3] La comparación tolera mayúsculas y espacios")
suelto = pd.DataFrame({"dsVendedor": ["  food   caba ", "Retail Ventas"]})
chk(set(dp.normalizar_vendedor(suelto)["dsVendedor"])
    == {"CASTILLON AGUSTIN DAMIAN", "SANABRIA, GONZALO"},
    "'  food   caba ' y 'Retail Ventas' también se traducen")

print("\n[4] Serie y cartera guardadas con el nombre viejo")
serie_vieja = pd.DataFrame({
    "anio_mes": ["2025-03", "2025-03"],
    "dsCanalMkt": ["FOOD SERVICE", "RETAIL"],
    "dsSubcanalMKT": ["SUB", "SUB"],
    "dsVendedor": ["FOOD CABA", "RETAIL VENTAS"],
    "region": ["CABA", "CABA"], "marca_linea": ["M", "M"],
    "kilos": [5.0, 7.0], "subtotalNeto": [1.0, 1.0],
    "costo": [0.0, 0.0], "cm": [1.0, 1.0],
    "clientes": [2, 3], "comprobantes": [2, 3],
})
al, _f = dp.alinear_serie(serie_vieja)
chk(set(al["dsVendedor"]) == {"CASTILLON AGUSTIN DAMIAN", "SANABRIA, GONZALO"},
    "alinear_serie traduce los meses viejos al leerlos")
chk(al["kilos"].sum() == serie_vieja["kilos"].sum(),
    "sin perder kilos en el camino")

cart_vieja = pd.DataFrame({
    "anio_mes": ["2025-03"] * 3,
    "dsCanalMkt": ["FOOD SERVICE"] * 3, "dsSubcanalMKT": ["SUB"] * 3,
    "dsVendedor": ["FOOD CABA", "CASTILLON AGUSTIN DAMIAN", "FOOD CABA"],
    "region": ["CABA"] * 3, "marca_linea": ["M"] * 3,
    "idCliente": ["C1", "C1", "C2"], "idArticulo": ["A1", "A1", "A2"],
})
alc, _f2 = dp.alinear_cartera(cart_vieja)
chk(set(alc["dsVendedor"]) == {"CASTILLON AGUSTIN DAMIAN"},
    "alinear_cartera también traduce")
m = dp.metricas_cartera(alc, "dsVendedor")
chk(len(m) == 1 and int(m["clientes"].iloc[0]) == 2,
    "fusionados, C1 se cuenta UNA vez (2 clientes, no 3)",
    str(m.to_dict("records")))

print("\n[5] Consecuencia: Food Caba hereda los días de Castillón")
chk(dp.dias_facturacion_vendedor("FOOD SERVICE", "CASTILLON AGUSTIN DAMIAN")
    is not None,
    f"Castillón tiene días declarados: "
    f"{dp.etiqueta_dias_facturacion('CASTILLON AGUSTIN DAMIAN')}")
sin_dias = dp.vendedores_sin_dias_facturacion(
    pd.DataFrame({"dsCanalMkt": ["FOOD SERVICE"], "dsVendedor": ["FOOD CABA"]}))
chk(sin_dias == ["FOOD CABA"],
    "sin normalizar, FOOD CABA figura como 'sin días declarados'")
sin_dias2 = dp.vendedores_sin_dias_facturacion(dp.normalizar_vendedor(
    pd.DataFrame({"dsCanalMkt": ["FOOD SERVICE"], "dsVendedor": ["FOOD CABA"]})))
chk(sin_dias2 == [], "normalizado, ya no: proyecta con lunes y viernes")

print("\n[6] Bordes")
chk(dp.normalizar_vendedor(pd.DataFrame()) is not None, "df vacío no rompe")
sin_col = pd.DataFrame({"otra": [1]})
chk(dp.normalizar_vendedor(sin_col) is sin_col,
    "un df sin columna dsVendedor vuelve tal cual")
nada = pd.DataFrame({"dsVendedor": ["COLOMBO, CARLOS"]})
chk(dp.normalizar_vendedor(nada) is nada,
    "si el mapa no aplica a ninguna fila, ni copia el df")

print("\n[7] Datos reales en disco")
if os.path.exists(dp.PARQUET_PATH):
    real = pd.read_parquet(dp.PARQUET_PATH)
    rn = dp.normalizar_vendedor(real)
    genericos = [v for v in rn["dsVendedor"].unique() if v in dp.MAPA_VENDEDORES]
    chk(not genericos, "no queda ningún nombre genérico en el detalle real")
    chk(len(rn) == len(real)
        and round(rn["kilos"].sum(), 6) == round(real["kilos"].sum(), 6),
        "mismas filas y mismos kilos que antes del renombre")
    gr = rn.groupby("dsVendedor")["kilos"].sum()
    for nombre in dp.MAPA_VENDEDORES.values():
        if nombre in gr.index:
            print(f"         {nombre}: {gr[nombre]:,.0f} kg")
else:
    print("  (sin parquet de detalle: se omite)")

print("\n" + ("TODO OK" if not fallas else f"{fallas} FALLA(S)"))
sys.exit(1 if fallas else 0)
