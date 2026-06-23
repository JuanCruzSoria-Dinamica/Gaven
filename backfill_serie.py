"""
backfill_serie.py
-----------------
Construye la serie mensual histórica (data/serie_mensual.parquet).

Recorre mes por mes desde SERIE_DESDE (enero 2025) hasta el mes actual, trae las
ventas de cada mes del API de Chess, las procesa con la MISMA limpieza que el
pipeline normal (data_pipeline.preparar) y las agrega a nivel mes × canal ×
subcanal (data_pipeline.agregar_serie).

RESISTENTE Y REANUDABLE:
  - Guarda CADA mes apenas lo termina (upsert atómico). Si se corta la conexión,
    lo ya bajado queda guardado.
  - Al volver a correrlo, OMITE los meses que ya están en la serie y sigue desde
    donde quedó. El mes en curso siempre se vuelve a traer (está incompleto).
  - Reintenta automáticamente si el servidor corta la conexión (con re-login).

Por qué un script aparte del cron:
  - El cron (data_pipeline.py) NO trae histórico: solo el mes actual + anterior
    y hace un "upsert" sobre la serie que deja este backfill.
  - Esto se corre una vez (o cuando quieras reconstruir la serie desde cero).

Uso:
    python backfill_serie.py            # normal / reanudar
    python backfill_serie.py --reset    # ignora lo existente y rehace todo

Credenciales: las mismas que usa data_pipeline.py (env vars o secrets.toml).
OJO: tarda (una consulta por mes). Es normal. Solo se hace una vez.
"""

import os
import sys
import time
import datetime as dt

import requests
import pandas as pd

import data_pipeline as dp


# Cuántas veces reintentar un mes si el servidor corta, y cuánto esperar.
MAX_REINTENTOS = 5
PAUSA_BASE_SEG = 6        # espera = PAUSA_BASE_SEG * número de intento
PAUSA_ENTRE_MESES = 1.5   # respiro entre meses para no saturar al servidor


def ventanas_mensuales(desde, hasta):
    """Lista de (primer_dia, ultimo_dia) para cada mes calendario entre `desde`
    y `hasta`. El último mes se corta en `hasta` (normalmente hoy)."""
    ventanas = []
    cursor = desde.replace(day=1)
    while cursor <= hasta:
        if cursor.month == 12:
            primer_dia_sig = dt.date(cursor.year + 1, 1, 1)
        else:
            primer_dia_sig = dt.date(cursor.year, cursor.month + 1, 1)
        fin_mes = primer_dia_sig - dt.timedelta(days=1)
        ventanas.append((cursor, min(fin_mes, hasta)))
        cursor = primer_dia_sig
    return ventanas


def traer_mes_con_reintentos(cfg, headers, fd, fh):
    """Trae un mes reintentando si el servidor corta la conexión. Devuelve
    (df_mes, headers) — headers puede renovarse si hubo que re-loguear.
    Lanza RuntimeError si agota los reintentos."""
    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            return dp.traer_ventas(cfg["base_url"], headers, fd, fh), headers
        except requests.exceptions.RequestException as e:
            if intento == MAX_REINTENTOS:
                raise RuntimeError(
                    f"No se pudo traer {fd} → {fh} tras {MAX_REINTENTOS} "
                    f"intentos. Último error: {type(e).__name__}: {e}"
                )
            espera = PAUSA_BASE_SEG * intento
            print(f"    ⚠  intento {intento}/{MAX_REINTENTOS} falló "
                  f"({type(e).__name__}). Reintento en {espera}s...")
            time.sleep(espera)
            # Re-login por si la sesión caducó al cortarse la conexión.
            try:
                headers = dp.login(cfg["base_url"], cfg["usuario"],
                                   cfg["password"])
            except Exception as e2:
                print(f"       (re-login falló: {type(e2).__name__}; "
                      f"se reintenta igual)")
    # No debería llegar acá.
    raise RuntimeError(f"No se pudo traer {fd} → {fh}")


def meses_ya_guardados():
    """Conjunto de 'YYYY-MM' que ya están en la serie (para reanudar)."""
    if not os.path.exists(dp.SERIE_PATH):
        return set()
    try:
        return set(pd.read_parquet(dp.SERIE_PATH)["anio_mes"].unique())
    except Exception:
        return set()


def main():
    reset = "--reset" in sys.argv
    if reset and os.path.exists(dp.SERIE_PATH):
        os.remove(dp.SERIE_PATH)
        print("(--reset) Serie anterior borrada; se reconstruye desde cero.")

    cfg = dp.cargar_credenciales()
    headers = dp.login(cfg["base_url"], cfg["usuario"], cfg["password"])

    hoy = dt.date.today()
    mes_actual = hoy.strftime("%Y-%m")
    ventanas = ventanas_mensuales(dp.SERIE_DESDE, hoy)
    ya = meses_ya_guardados()

    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] Backfill serie mensual "
          f"({dp.SERIE_DESDE:%Y-%m} → {hoy:%Y-%m}) · {len(ventanas)} meses")
    if ya:
        print(f"  Reanudando: {len(ya)} mes(es) ya guardados se omiten.")

    procesados = 0
    for desde, hasta in ventanas:
        mes = desde.strftime("%Y-%m")
        # Omite meses ya guardados, salvo el mes en curso (siempre incompleto).
        if mes in ya and mes != mes_actual:
            print(f"  {mes}: ya estaba en la serie, se omite.")
            continue

        fd, fh = desde.strftime("%Y-%m-%d"), hasta.strftime("%Y-%m-%d")
        df_mes, headers = traer_mes_con_reintentos(cfg, headers, fd, fh)

        if df_mes.empty:
            print(f"  {mes}: 0 filas (se omite)")
            continue

        n_origen = len(df_mes)
        df_mes = dp.preparar(df_mes)
        # Guarda YA este mes (upsert atómico). Si se corta el siguiente, esto
        # queda persistido.
        dp.upsert_serie(df_mes)
        agg = dp.agregar_serie(df_mes)
        print(f"  {mes}: {n_origen} filas → {len(agg)} grupos · guardado ✓")
        procesados += 1
        time.sleep(PAUSA_ENTRE_MESES)

    if not os.path.exists(dp.SERIE_PATH):
        print("No se guardó nada (el API no devolvió datos).")
        return

    serie = pd.read_parquet(dp.SERIE_PATH)
    meses = sorted(serie["anio_mes"].unique())
    print(f"\nOK: {len(serie)} filas en {dp.SERIE_PATH}")
    print(f"    Meses: {meses[0]} → {meses[-1]} ({len(meses)} meses) · "
          f"{procesados} traídos en esta corrida")


if __name__ == "__main__":
    main()
