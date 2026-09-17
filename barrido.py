"""Barrido de calidad de dato sobre las ventas de Chess. Bajo demanda.

Busca líneas de facturación que casi seguro son un error de carga y no una
decisión comercial: margen negativo, costo faltante, margen absurdamente alto,
saltos de costo de un mes a otro. La idea es tener la lista concreta para ir a
corregirla en Chess, no un indicador agregado.

Arma un Excel con tres hojas:
    Resumen   - una fila por regla: cuántas líneas, cuánta plata, qué impacto
    Por SKU   - agrupado por regla + artículo, que es el grano al que se
                arregla el costo en Chess
    Detalle   - la línea cruda con el comprobante, para ir a buscarla

Usa el MISMO costo que el tablero (acuerdos McCain + bonificación por canal
aplicados sobre el costo crudo del parquet), así los números cierran con
gaven.dinamicaeye.com y no hay que explicar diferencias.

No escribe ni toca el parquet: es de solo lectura.

Uso:
    python3 barrido.py                    # el último mes cerrado
    python3 barrido.py --mes 2026-03      # un mes puntual
    python3 barrido.py --desde 2026-07-01 --hasta 2026-08-31
    python3 barrido.py --salida C:/ruta/revision.xlsx
"""
import argparse
import calendar
import datetime as dt
import io
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import numpy as np                         # noqa: E402
import pandas as pd                        # noqa: E402

import data_pipeline as dp                 # noqa: E402


# ---------------------------------------------------------------------------
# Umbrales
# ---------------------------------------------------------------------------
# Calibrados sobre el histórico real del parquet (sep 2026), no a ojo. La
# distribución del CM% por línea de factura es: mediana 22 %, percentil 99
# 40 %, percentil 99,9 54 %. Por eso el techo está en 60 y no en 90: con 90
# el barrido no encuentra NADA salvo el caso de costo cero, y un barrido que
# nunca salta se lee como "está todo bien" cuando en realidad no miró.
# Sep 2026: umbrales pedidos por gestión comercial para la revisión del
# viernes: margen menor a 5 % y mayor a 40 % (antes 0 % y 60 %).
CM_ALTO = 40.0          # CM % por línea por encima de esto = sospechoso
CM_BAJO = 5.0           # CM % por debajo de esto = sospechoso
SALTO_COSTO = 40.0      # variación % del costo unitario de un SKU mes a mes
DESVIO_PRECIO = 50.0    # desvío % del precio de venta vs la mediana del SKU

# Las reglas de precio de venta son las más ruidosas (un mismo SKU se vende
# legítimamente a precios distintos por canal y por acuerdo). Se puede apagar
# sin tocar nada más.
INCLUIR_PRECIO_ATIPICO = True

# Piso de facturación para que una línea entre en las reglas de desvío: por
# debajo de esto el error existe pero no mueve la aguja y solo agrega ruido.
PISO_FACTURACION = 1000.0


def _ars(x, dec=0):
    """Número con separador de miles a la argentina: 1.234.567."""
    try:
        return f"{x:,.{dec}f}".replace(",", "@").replace(".", ",").replace("@", ".")
    except (TypeError, ValueError):
        return str(x)


COLS_DETALLE = [
    "regla", "motivo", "fechaComprobate", "dsDocumento", "nrodoc",
    "idCliente", "nombreCliente", "dsCanalMkt", "dsVendedor",
    "idArticulo", "dsArticulo", "proveedor",
    "kilos_cargo", "bultos_cargo", "precioUnitarioNeto", "preciocomprant",
    "subtotalNeto", "costo_unitario", "cm", "cm_pct",
]


# ---------------------------------------------------------------------------
# Carga
# ---------------------------------------------------------------------------
def cargar():
    """Lee el parquet y le aplica los mismos ajustes que la app.

    Replica cargar_datos_local() de app.py: marca/línea derivada del lookup,
    acuerdos McCain y bonificación por canal. Si esto se desincroniza de la
    app, los números del barrido dejan de cerrar con el tablero.
    """
    df = pd.read_parquet(dp.PARQUET_PATH)
    df = dp.agregar_marca_linea(df)
    df = dp.aplicar_acuerdos(df)
    df = dp.aplicar_bonificacion_canal(df)
    df["fechaComprobate"] = pd.to_datetime(df["fechaComprobate"])
    return df


def ventana(args, df):
    """(desde, hasta) según los argumentos, o el último mes cerrado."""
    if args.desde and args.hasta:
        return (dt.date.fromisoformat(args.desde),
                dt.date.fromisoformat(args.hasta))
    if args.mes:
        anio, mes = (int(x) for x in args.mes.split("-"))
    else:
        hoy = dt.date.today()
        primero = hoy.replace(day=1)
        cierre = primero - dt.timedelta(days=1)   # último día del mes anterior
        anio, mes = cierre.year, cierre.month
    ult = calendar.monthrange(anio, mes)[1]
    return dt.date(anio, mes, 1), dt.date(anio, mes, ult)


def recortar(df, desde, hasta):
    f = df["fechaComprobate"]
    return df[(f >= pd.Timestamp(desde)) & (f <= pd.Timestamp(hasta))].copy()


def preparar(df):
    """Agrega cm / cm_pct y marca qué líneas tienen cargo.

    `con_cargo`: la línea se cobró. La mercadería bonificada va con costo 0 a
    propósito (el proveedor la repone, ver costo_bruto en data_pipeline), así
    que si no se excluye, cada bonificación aparece como "costo faltante" y el
    barrido se vuelve inservible.
    """
    df["cm"] = df["subtotalNeto"] - df["costo_unitario"]
    df["cm_pct"] = np.where(
        df["subtotalNeto"] != 0, df["cm"] / df["subtotalNeto"] * 100, np.nan
    )
    df["con_cargo"] = (df["kilos_cargo"] != 0) | (df["bultos_cargo"] != 0)
    return df


# ---------------------------------------------------------------------------
# Reglas
# ---------------------------------------------------------------------------
# Cada regla devuelve las líneas sospechosas con dos columnas agregadas:
# `regla` (el nombre corto) y `motivo` (por qué saltó ESA línea, en castellano,
# con el número que la hizo saltar). El motivo es lo que hace que el Excel se
# pueda leer sin tener que venir a leer este archivo.

def r_margen_negativo(fac):
    s = fac[(fac["subtotalNeto"] > 0) & (fac["cm_pct"] < CM_BAJO)].copy()
    s["regla"] = f"Margen bajo (< {CM_BAJO:g} %)"
    s["motivo"] = s.apply(
        lambda r: (f"Se vendió a $ {_ars(r['precioUnitarioNeto'])} con un costo "
                   f"de $ {_ars(r['preciocomprant'])} (CM {r['cm_pct']:.1f} %)"),
        axis=1,
    )
    return s


def r_costo_faltante(fac):
    """Línea cobrada pero sin costo: casi siempre el artículo no tiene precio
    de compra cargado en Chess. Es el caso que más distorsiona el margen,
    porque el 100 % de la venta se lee como contribución."""
    s = fac[(fac["subtotalNeto"] > 0) & fac["con_cargo"]
            & (fac["costo_unitario"] == 0)].copy()
    s["regla"] = "Costo faltante"
    s["motivo"] = s.apply(
        lambda r: (f"Línea cobrada sin costo: facturó "
                   f"$ {_ars(r['subtotalNeto'])} y el costo quedó en cero"),
        axis=1,
    )
    return s


def r_margen_alto(fac):
    """CM % por encima del techo, descontando las de costo cero (esas ya
    salen por su propia regla y duplicarlas infla el resumen)."""
    s = fac[(fac["subtotalNeto"] > 0) & (fac["cm_pct"] > CM_ALTO)
            & (fac["costo_unitario"] != 0)
            & (fac["subtotalNeto"] >= PISO_FACTURACION)].copy()
    s["regla"] = f"Margen alto (> {CM_ALTO:g} %)"
    s["motivo"] = s.apply(
        lambda r: (f"CM {r['cm_pct']:.1f} %, por encima del techo de {CM_ALTO:g} %"),
        axis=1,
    )
    return s


def r_salto_costo(fac, previo):
    """Costo unitario de compra de un SKU que se mueve fuerte contra el mes
    anterior. No juzga si el salto está bien: lo pone arriba de la mesa."""
    if previo.empty:
        return pd.DataFrame(columns=fac.columns)

    def costo_medio(d):
        d = d[(d["preciocomprant"] > 0) & d["con_cargo"]]
        return d.groupby("idArticulo")["preciocomprant"].median()

    ant = costo_medio(previo)
    if ant.empty:
        return pd.DataFrame(columns=fac.columns)

    s = fac[(fac["preciocomprant"] > 0) & fac["con_cargo"]].copy()
    s["_costo_ant"] = s["idArticulo"].map(ant)
    s = s[s["_costo_ant"].notna() & (s["_costo_ant"] > 0)]
    s["_var"] = (s["preciocomprant"] - s["_costo_ant"]) / s["_costo_ant"] * 100
    s = s[s["_var"].abs() > SALTO_COSTO]
    # Una sola línea por SKU: el salto es del artículo, no de la venta. Se
    # deja la de mayor facturación como evidencia.
    s = s.sort_values("subtotalNeto", ascending=False).drop_duplicates("idArticulo")
    s["regla"] = "Salto de costo"
    s["motivo"] = s.apply(
        lambda r: (f"El costo pasó de $ {_ars(r['_costo_ant'])} a "
                   f"$ {_ars(r['preciocomprant'])} ({r['_var']:+.0f} %) "
                   f"contra el mes anterior"),
        axis=1,
    )
    return s.drop(columns=["_costo_ant", "_var"])


def r_precio_atipico(fac):
    """Precio de venta lejos de la mediana del mismo SKU en el mismo mes.
    La más ruidosa de todas: un SKU se vende legítimamente a precios distintos
    por canal. Sirve para pescar el cero de más o el de menos al tipear."""
    s = fac[(fac["subtotalNeto"] >= PISO_FACTURACION)
            & (fac["precioUnitarioNeto"] > 0)].copy()
    if s.empty:
        return s
    med = s.groupby("idArticulo")["precioUnitarioNeto"].transform("median")
    n = s.groupby("idArticulo")["precioUnitarioNeto"].transform("size")
    s["_desvio"] = (s["precioUnitarioNeto"] - med) / med * 100
    s["_med"] = med
    # Con menos de 5 ventas del SKU en el mes la mediana no dice nada.
    s = s[(n >= 5) & (s["_desvio"].abs() > DESVIO_PRECIO)].copy()
    if s.empty:
        # Ojo: asignar una Serie a un DataFrame vacío le copia el índice
        # entero y devuelve miles de filas en blanco. Se corta acá.
        return s.drop(columns=["_desvio", "_med"])
    s["regla"] = "Precio de venta atípico"
    s["motivo"] = s.apply(
        lambda r: (f"Se facturó a $ {_ars(r['precioUnitarioNeto'])} cuando el "
                   f"resto del mes fue $ {_ars(r['_med'])} "
                   f"({r['_desvio']:+.0f} %)"),
        axis=1,
    )
    return s.drop(columns=["_desvio", "_med"])


# ---------------------------------------------------------------------------
# Armado
# ---------------------------------------------------------------------------
def barrer(df, desde, hasta):
    """Corre todas las reglas y devuelve (detalle, notas)."""
    per = preparar(recortar(df, desde, hasta))
    notas = []

    # Las notas de crédito quedan afuera: con subtotal negativo el CM % se da
    # vuelta y toda línea devuelta saltaría como "margen negativo". Se cuentan
    # acá para que la exclusión sea visible y no un silencio.
    nc = per[per["dsDocumento"] == "NOTA DE CREDITO"]
    if len(nc):
        notas.append(
            f"{len(nc)} nota(s) de crédito excluidas del barrido "
            f"($ {_ars(abs(nc['subtotalNeto'].sum()))}); las reglas de margen "
            f"no aplican sobre comprobantes negativos."
        )

    anul = per[per["anulado"].astype(str).str.upper() != "NO"]
    if len(anul):
        notas.append(f"{len(anul)} línea(s) anuladas excluidas.")

    fac = per[(per["dsDocumento"] != "NOTA DE CREDITO")
              & (per["anulado"].astype(str).str.upper() == "NO")]

    ini_prev = (desde.replace(day=1) - dt.timedelta(days=1)).replace(day=1)
    fin_prev = desde - dt.timedelta(days=1)
    previo = preparar(recortar(df, ini_prev, fin_prev))
    previo = previo[previo["dsDocumento"] != "NOTA DE CREDITO"]

    partes = [
        r_margen_negativo(fac),
        r_costo_faltante(fac),
        r_margen_alto(fac),
        r_salto_costo(fac, previo),
    ]
    if INCLUIR_PRECIO_ATIPICO:
        partes.append(r_precio_atipico(fac))

    partes = [p for p in partes if len(p)]
    if not partes:
        return pd.DataFrame(columns=COLS_DETALLE), notas, len(fac)

    det = pd.concat(partes, ignore_index=True)
    det = det.reindex(columns=COLS_DETALLE)
    det = det.sort_values(["regla", "subtotalNeto"], ascending=[True, False])
    return det.reset_index(drop=True), notas, len(fac)


def resumir(det):
    if det.empty:
        return pd.DataFrame()
    r = (det.groupby("regla")
         .agg(lineas=("regla", "size"),
              comprobantes=("nrodoc", "nunique"),
              skus=("idArticulo", "nunique"),
              clientes=("idCliente", "nunique"),
              facturacion=("subtotalNeto", "sum"),
              impacto_cm=("cm", "sum"))
         .reset_index()
         .sort_values("lineas", ascending=False))
    return r


def por_sku(det):
    if det.empty:
        return pd.DataFrame()
    r = (det.groupby(["regla", "idArticulo", "dsArticulo", "proveedor"])
         .agg(lineas=("regla", "size"),
              clientes=("idCliente", "nunique"),
              facturacion=("subtotalNeto", "sum"),
              impacto_cm=("cm", "sum"),
              cm_pct_min=("cm_pct", "min"),
              cm_pct_max=("cm_pct", "max"))
         .reset_index()
         .sort_values(["regla", "facturacion"], ascending=[True, False]))
    return r


# ---------------------------------------------------------------------------
# Excel
# ---------------------------------------------------------------------------
def escribir_excel(ruta, hojas):
    from openpyxl.utils import get_column_letter

    plata = {"facturacion", "impacto_cm", "subtotalNeto", "costo_unitario",
             "cm", "precioUnitarioNeto", "preciocomprant"}
    pct = {"cm_pct", "cm_pct_min", "cm_pct_max"}

    with pd.ExcelWriter(ruta, engine="openpyxl") as w:
        vacio = True
        for nombre, t in hojas.items():
            if t is None or len(t) == 0:
                continue
            vacio = False
            t.to_excel(w, sheet_name=str(nombre)[:31], index=False)
            ws = w.sheets[str(nombre)[:31]]
            ws.freeze_panes = "A2"
            for i, c in enumerate(t.columns, 1):
                letra = get_column_letter(i)
                if pd.api.types.is_datetime64_any_dtype(t[c]):
                    for celda in ws[letra][1:]:
                        celda.number_format = "DD/MM/YYYY"
                    ws.column_dimensions[letra].width = 12
                    continue
                ancho = max([len(str(c))]
                            + t[c].astype(str).str.len().head(200).tolist())
                ws.column_dimensions[letra].width = min(max(ancho + 2, 10), 55)
                if not pd.api.types.is_numeric_dtype(t[c]):
                    continue
                fmt = ('"$" #,##0' if c in plata
                       else '0.0"%"' if c in pct else "#,##0")
                for celda in ws[letra][1:]:
                    celda.number_format = fmt
        if vacio:
            pd.DataFrame({"Resultado": ["Sin hallazgos en el período."]}).to_excel(
                w, sheet_name="Resumen", index=False
            )


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mes", help="YYYY-MM. Por defecto, el último mes cerrado.")
    ap.add_argument("--desde", help="YYYY-MM-DD (usar junto con --hasta)")
    ap.add_argument("--hasta", help="YYYY-MM-DD")
    ap.add_argument("--salida", help="Ruta del .xlsx. Por defecto barrido_<mes>.xlsx")
    args = ap.parse_args()

    df = cargar()
    desde, hasta = ventana(args, df)
    ult_dato = df["fechaComprobate"].max().date()

    print(f"\nBarrido de calidad de dato · {desde:%d/%m/%Y} → {hasta:%d/%m/%Y}")
    print(f"Parquet con datos hasta el {ult_dato:%d/%m/%Y}")
    if ult_dato < hasta:
        print("  ATENCIÓN: el parquet no llega al final del período. "
              "Corré el pipeline antes de pasar esto.")

    det, notas, n_fac = barrer(df, desde, hasta)
    res = resumir(det)

    print(f"\n{_ars(n_fac)} líneas de factura revisadas\n")
    if det.empty:
        print("  Sin hallazgos.")
    else:
        for _, r in res.iterrows():
            print(f"  {r['regla']:<24} {r['lineas']:>5} línea(s)  ·  "
                  f"{r['skus']:>3} SKU  ·  $ {_ars(r['facturacion']):>14}")
    for n in notas:
        print(f"\n  nota: {n}")

    salida = args.salida or f"barrido_{desde:%Y-%m}.xlsx"
    escribir_excel(salida, {
        "Resumen": res,
        "Por SKU": por_sku(det),
        "Detalle": det,
    })
    print(f"\nExcel: {os.path.abspath(salida)}\n")


if __name__ == "__main__":
    main()
