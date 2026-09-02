"""
conciliacion_chess.py
---------------------
DE-PARA entre el "REPORTE DE COMISIONES POR VENTA" de Chess (el que saca
Cristina) y el Tablero Comercial de Gaven.

Los dos miden el mismo mes y NO dan igual, y está bien que no den igual: el
tablero saca cosas a propósito (réplica del Excel comercial) y además mide los
kilos con otra columna. Este script pone esa diferencia en números, paso por
paso, para que se pueda decir "faltan X kg y son estos".

Qué hace:
  1. Baja de la API el mes pedido SIN filtrar (lo mismo que ve Chess).
  2. Arma la CASCADA: cuánto saca cada filtro del tablero, uno por uno.
  3. Muestra las tres definiciones de "kilos" que conviven, para saber contra
     cuál se está comparando.
  4. Arma la tabla POR VENDEDOR con el mismo layout del reporte de Chess
     (bultos / unidad de medida / importe), crudo y con los filtros del
     tablero, con la diferencia al lado.

NO escribe nada: es de solo lectura (no toca el parquet, la serie ni las
metas). Usa las mismas credenciales que el resto del proyecto.

Uso:
    python3 conciliacion_chess.py                # mes anterior completo
    python3 conciliacion_chess.py --mes 2026-08
"""

import argparse
import datetime as dt
import calendar

import pandas as pd

import data_pipeline as dp


# ---------------------------------------------------------------------------
# De-para de columnas: qué columna del reporte de Chess mira cada métrica.
# ---------------------------------------------------------------------------
# BULTOS            -> cantidadesTotal   (todo, con cargo y bonificado)
# UNIDAD DE MEDIDA  -> unimedtotal       (kg o unidades según el artículo)
# IMPORTE           -> subtotalNeto (sin IVA) o subtotalFinal (con IVA):
#                      cuál de las dos es se resuelve mirando el print, no de
#                      memoria. El tablero factura SIEMPRE con subtotalNeto.
DE_PARA_COLUMNAS = [
    ("BULTOS", "cantidadesTotal"),
    ("UNIDAD DE MEDIDA", "unimedtotal"),
    ("IMPORTE (¿neto?)", "subtotalNeto"),
    ("IMPORTE (¿con IVA?)", "subtotalFinal"),
]

# Los filtros de dp.preparar(), abiertos uno por uno y EN EL MISMO ORDEN, para
# poder cobrarle a cada uno lo que se lleva. Si se toca preparar(), tocar acá:
# el control del final avisa si quedaron desalineados.
FILTROS_TABLERO = [
    ("Comprobantes ANULADOS",
     lambda d: d["anulado"].astype(str).str.upper().str.strip() != "NO"),
    ("Canal VIANDAS",
     lambda d: d["dsCanalMkt"].astype(str).str.upper().str.strip() == "VIANDAS"),
    ("Vendedor DIRECTA",
     lambda d: d["dsVendedor"].astype(str).str.upper().str.strip() == "DIRECTA"),
    ("Subcanal VIANDAS",
     lambda d: d["dsSubcanalMKT"].astype(str).str.upper().str.strip() == "VIANDAS"),
    ("Clientes excluidos (%s)" % ", ".join(map(str, dp.CLIENTES_EXCLUIR)),
     lambda d: d["idCliente"].isin(dp.CLIENTES_EXCLUIR)),
    ("Artículos excluidos (%s: conceptos y viandas)"
     % ", ".join(map(str, dp.ARTICULOS_EXCLUIR)),
     lambda d: pd.to_numeric(d["idArticulo"], errors="coerce")
     .isin(dp.ARTICULOS_EXCLUIR)),
]

# Métricas que se van midiendo en cada paso de la cascada.
METRICAS = ["cantidadesTotal", "unimedtotal", "pesoTotal", "kilos",
            "subtotalNeto"]


def normalizar_crudo(df):
    """Deja el crudo de la API comparable SIN filtrar nada: recorta a las
    columnas de interés, pasa los números a número y deriva 'kilos' con la
    misma fórmula del tablero. Es dp.preparar() menos los filtros."""
    cols = [c for c in dp.COLUMNAS_IMPORTANTES if c in df.columns]
    df = df[cols].copy()
    for c in dp.COLUMNAS_NUMERICAS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["fechaComprobate"] = pd.to_datetime(df["fechaComprobate"],
                                           errors="coerce")
    return dp.recalcular_costo(df)


def cascada(df_crudo):
    """Cuánto se lleva cada filtro del tablero, aplicados en orden sobre lo
    que va quedando. Devuelve un DataFrame con una fila por paso más la fila
    'CRUDO (= Chess)' arriba y 'TABLERO' abajo."""
    filas = [dict(Paso="CRUDO (= Chess)", Filas=len(df_crudo),
                  **{m: df_crudo[m].sum() for m in METRICAS})]
    queda = df_crudo
    for nombre, mask in FILTROS_TABLERO:
        fuera = queda[mask(queda)]
        queda = queda[~mask(queda)]
        filas.append(dict(Paso="  (-) " + nombre, Filas=-len(fuera),
                          **{m: -fuera[m].sum() for m in METRICAS}))
    filas.append(dict(Paso="TABLERO", Filas=len(queda),
                      **{m: queda[m].sum() for m in METRICAS}))
    return pd.DataFrame(filas), queda


def definiciones_de_kilos(df):
    """Las tres formas de contar 'kilos' que conviven. Sirve para saber si la
    diferencia es de FILAS (filtros) o de COLUMNA (qué se llama kilo).

    unimedtotal  -> lo que muestra Chess en 'UNIDAD DE MEDIDA'.
    pesoTotal    -> el peso real, 0 en los artículos que no pesan.
    kilos        -> lo que muestra el tablero: pesoTotal y, si es 0,
                    unimedtotal (ver dp.recalcular_costo).
    """
    pes = df[df["pesoTotal"] != 0]
    nop = df[df["pesoTotal"] == 0]
    return pd.DataFrame([
        dict(Definicion="unimedtotal (Chess: UNIDAD DE MEDIDA)",
             Total=df["unimedtotal"].sum(),
             Pesables=pes["unimedtotal"].sum(),
             No_pesables=nop["unimedtotal"].sum()),
        dict(Definicion="pesoTotal (peso real)",
             Total=df["pesoTotal"].sum(),
             Pesables=pes["pesoTotal"].sum(),
             No_pesables=nop["pesoTotal"].sum()),
        dict(Definicion="kilos (Tablero: peso, o unimed si no pesa)",
             Total=df["kilos"].sum(),
             Pesables=pes["kilos"].sum(),
             No_pesables=nop["kilos"].sum()),
        dict(Definicion="unimedcargo (solo lo cobrado)",
             Total=df["unimedcargo"].sum(),
             Pesables=pes["unimedcargo"].sum(),
             No_pesables=nop["unimedcargo"].sum()),
    ])


def por_vendedor(df_crudo, df_tablero):
    """Tabla con el layout del reporte de Chess (bultos / unidad de medida /
    importe) y, al lado, lo mismo ya filtrado por el tablero y en kilos de
    tablero. Una fila por vendedor, ordenada por lo que más se pierde."""
    def _g(d, sufijo):
        g = (d.groupby(d["dsVendedor"].astype(str).str.strip(), dropna=False)
             .agg(**{f"Bultos{sufijo}": ("cantidadesTotal", "sum"),
                     f"UM{sufijo}": ("unimedtotal", "sum"),
                     f"Kilos{sufijo}": ("kilos", "sum"),
                     f"Importe{sufijo}": ("subtotalNeto", "sum")}))
        g.index.name = "Vendedor"
        return g

    t = (_g(df_crudo, " Chess")
         .join(_g(df_tablero, " Tablero"), how="outer")
         .fillna(0.0))
    t["Dif UM"] = t["UM Tablero"] - t["UM Chess"]
    t["Dif Kilos vs UM Chess"] = t["Kilos Tablero"] - t["UM Chess"]
    return t.sort_values("Dif UM").reset_index()


def revisar_prefijos(df):
    """Los filtros del tablero comparan contra el nombre pelado ('DIRECTA',
    'VIANDAS'). Si la API devuelve el código adelante ('14 - DIRECTA'), esos
    filtros NO sacan nada y el tablero está contando de más. Se avisa acá
    porque no se puede saber sin mirar los datos reales."""
    avisos = []
    for col in ["dsVendedor", "dsCanalMkt", "dsSubcanalMKT"]:
        if col not in df.columns:
            continue
        vals = df[col].astype(str).str.strip()
        con_prefijo = vals[vals.str.match(r"^\d+\s*-\s*")].unique().tolist()
        if len(con_prefijo):
            avisos.append((col, sorted(con_prefijo)[:5]))
    return avisos


def _rango_mes(anio_mes):
    anio, mes = map(int, str(anio_mes).split("-"))
    ultimo = calendar.monthrange(anio, mes)[1]
    return dt.date(anio, mes, 1), dt.date(anio, mes, ultimo)


def _mes_anterior(hoy=None):
    hoy = hoy or dt.date.today()
    ult = hoy.replace(day=1) - dt.timedelta(days=1)
    return ult.strftime("%Y-%m")


def _p(df, titulo):
    print(f"\n== {titulo} ==")
    print(df.to_string(index=False,
                       float_format=lambda x: f"{x:,.2f}"))


def main(anio_mes=None):
    anio_mes = anio_mes or _mes_anterior()
    desde, hasta = _rango_mes(anio_mes)

    cfg = dp.cargar_credenciales()
    headers = dp.login(cfg["base_url"], cfg["usuario"], cfg["password"])
    print(f"Conciliación Chess ↔ Tablero · {anio_mes} "
          f"({desde:%d/%m/%Y} - {hasta:%d/%m/%Y})")

    df_raw = dp.traer_ventas(cfg["base_url"], headers,
                             desde.strftime("%Y-%m-%d"),
                             hasta.strftime("%Y-%m-%d"))
    if df_raw.empty:
        print("La API no devolvió filas para ese mes.")
        return
    print(f"Filas crudas de la API: {len(df_raw)}")

    crudo = normalizar_crudo(df_raw)

    _p(pd.DataFrame([dict(Columna=lbl, Campo_API=campo,
                          Total=crudo[campo].sum())
                     for lbl, campo in DE_PARA_COLUMNAS
                     if campo in crudo.columns]),
       "1) De-para de columnas · totales CRUDOS (compará contra el reporte)")

    casc, tablero = cascada(crudo)
    _p(casc, "2) Cascada: qué saca cada filtro del tablero")

    _p(definiciones_de_kilos(tablero),
       "3) Kilos: la misma venta contada de cuatro formas (ya filtrada)")

    _p(por_vendedor(crudo, tablero), "4) Vendedor por vendedor")

    # Control: la cascada tiene que terminar exactamente donde termina el
    # pipeline de verdad. Si no, alguien tocó preparar() y no tocó acá.
    real = dp.preparar(df_raw)
    ok = (len(real) == len(tablero)
          and abs(real["kilos"].sum() - tablero["kilos"].sum()) < 0.01)
    print(f"\nControl: la cascada coincide con dp.preparar() -> "
          f"{'OK' if ok else 'NO COINCIDE (revisar FILTROS_TABLERO)'}")

    avisos = revisar_prefijos(crudo)
    if avisos:
        print("\n*** OJO: la API devuelve estos campos con el código adelante:")
        for col, ejemplos in avisos:
            print(f"    {col}: {', '.join(ejemplos)}")
        print("    Los filtros de dp.preparar() comparan contra el nombre")
        print("    pelado ('DIRECTA', 'VIANDAS'), así que con prefijo NO")
        print("    sacan nada y el tablero los está contando.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="De-para Chess (comisiones por venta) ↔ Tablero Gaven")
    ap.add_argument("--mes", metavar="YYYY-MM", default=None,
                    help="Mes a conciliar (default: el mes anterior completo)")
    main(ap.parse_args().mes)
