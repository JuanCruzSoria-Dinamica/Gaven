"""
diagnostico_api.py
------------------
Chequeo rápido para saber si la API de Chess está trayendo ventas nuevas o si
simplemente no se cargaron ventas en los últimos días.

NO escribe nada (no toca el parquet ni la serie). Solo consulta y muestra
conteos. Usá las mismas credenciales que el resto del proyecto.

Uso:
    python diagnostico_api.py
"""

import datetime as dt
import data_pipeline as dp


def contar(cfg, headers, desde, hasta):
    """Filas CRUDAS que devuelve la API para un rango (sin filtros)."""
    df = dp.traer_ventas(cfg["base_url"], headers,
                         desde.strftime("%Y-%m-%d"),
                         hasta.strftime("%Y-%m-%d"))
    return len(df), df


def main():
    cfg = dp.cargar_credenciales()
    headers = dp.login(cfg["base_url"], cfg["usuario"], cfg["password"])
    hoy = dt.date.today()

    print(f"Hoy es {hoy:%Y-%m-%d} ({hoy:%A})\n")

    # 1) Conteo día por día de los últimos 8 días (incluye hoy).
    print("== Filas CRUDAS de la API, día por día (últimos 8 días) ==")
    for i in range(7, -1, -1):
        d = hoy - dt.timedelta(days=i)
        n, _ = contar(cfg, headers, d, d)
        marca = "  <-- HOY" if d == hoy else ""
        print(f"  {d:%Y-%m-%d} ({d:%a}): {n:>6} filas{marca}")

    # 2) Mes actual completo (día 1 -> hoy) y mes anterior completo.
    primer_actual = hoy.replace(day=1)
    ult_anterior = primer_actual - dt.timedelta(days=1)
    primer_anterior = ult_anterior.replace(day=1)

    n_act, df_act = contar(cfg, headers, primer_actual, hoy)
    n_ant, _ = contar(cfg, headers, primer_anterior, ult_anterior)
    print("\n== Totales por mes (lo mismo que trae el pipeline) ==")
    print(f"  Mes anterior {primer_anterior:%Y-%m} completo: {n_ant:>6} filas")
    print(f"  Mes actual   {primer_actual:%Y-%m} (1 -> hoy): {n_act:>6} filas")
    print(f"  Suma crudas (anterior + actual):            {n_ant + n_act:>6} filas")

    # 3) Hasta qué fecha llegan realmente los datos del mes actual.
    if not df_act.empty:
        f = dp.pd.to_datetime(df_act["fechaComprobate"], errors="coerce")
        print(f"\n  Fecha MÁS RECIENTE con ventas en el mes actual: "
              f"{f.max():%Y-%m-%d}")
        print("  Filas por día del mes actual:")
        for fecha, cnt in f.dt.date.value_counts().sort_index().items():
            print(f"    {fecha:%Y-%m-%d}: {cnt}")

    print("\nListo. Cómo leerlo:")
    print("  - Si los días viejos traen filas y HOY trae 0 (o pocas): la API")
    print("    anda bien, todavía no se cargaron ventas de hoy. NO es un bug.")
    print("  - Si TODOS los días traen 0: la API no está devolviendo datos")
    print("    (revisar credenciales / servicio de Chess).")


if __name__ == "__main__":
    main()
