"""Smoke test de la solapa Metas: corre app.py entero con AppTest, recorre los
tres niveles de seguimiento y guarda objetivos de canal, proveedor y vendedor
verificando que persistan y que los controles de consistencia los vean.

Trabaja sobre una copia de data/metas.parquet y la restaura al terminar.

Uso:  python3 smoke_metas.py
"""
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from streamlit.testing.v1 import AppTest  # noqa: E402
import data_pipeline as dp                # noqa: E402

CANAL = "FOOD SERVICE"
METAS = "data/metas.parquet"
HIST = "data/metas_historial.parquet"

fallas = 0


def chk(cond, msg):
    global fallas
    print(("  ok    " if cond else "  FALLA ") + msg)
    if not cond:
        fallas += 1


def excepciones(at, msg):
    chk(not at.exception, msg)
    for e in at.exception:
        print("        ", e.value)


# --- Backup -----------------------------------------------------------------
bk_metas = tempfile.mktemp(suffix=".parquet")
bk_hist = tempfile.mktemp(suffix=".parquet")
if os.path.exists(METAS):
    shutil.copy(METAS, bk_metas)
if os.path.exists(HIST):
    shutil.copy(HIST, bk_hist)


def restaurar():
    """Deja los parquet como estaban. Es idempotente y se llama apenas
    terminan los checks que escriben, no solo en el finally: si el proceso se
    corta antes (falta de memoria, Ctrl-C), los datos reales ya están a salvo.

    No borra con os.remove porque la carpeta puede estar sincronizada
    (OneDrive) y no permitirlo; sobreescribe en su lugar."""
    for orig, dest in ((bk_metas, METAS), (bk_hist, HIST)):
        try:
            if os.path.exists(orig):
                shutil.copy(orig, dest)
            elif os.path.exists(dest):
                dp._metas_vacio().to_parquet(dest, index=False)
        except Exception as exc:              # noqa: BLE001
            print(f"  aviso: no se pudo restaurar {dest}: {exc}")


try:
    print("\nSmoke test · solapa Metas")

    # Se siembra un presupuesto anual para que la grilla del presupuesto
    # arranque con valores y el botón de guardar tenga algo que hacer.
    import pandas as pd
    ANIO = "2026"
    MESES = [f"{ANIO}-{i:02d}" for i in range(1, 13)]
    dp.upsert_metas(
        pd.DataFrame([{"anio_mes": ms, "dsCanalMkt": CANAL, "meta_kg": 5000}
                      for ms in MESES]),
        anio_mes=MESES, canales=[CANAL], tipo="presupuesto", nivel="canal",
        historial=False)

    at = AppTest.from_file("app.py", default_timeout=300)
    # El tablero tiene login (st.stop() si no hay rol). Se saltea sembrando el
    # rol en session_state, igual que después de un login exitoso.
    at.session_state["rol"] = "supervisor"
    at.run()
    excepciones(at, "el tablero levanta")

    # --- Seguimiento en los tres niveles, vista consolidada -----------------
    for nivel in ["Canal", "Proveedor / línea", "Vendedor"]:
        at.radio(key="metas_nivel_seg").set_value(nivel)
        at.run()
        excepciones(at, f"vista TODOS abierta por {nivel}")

    # --- Un canal concreto --------------------------------------------------
    at.selectbox(key="metas_canal").select(CANAL)
    at.run()
    excepciones(at, f"solapa Metas con el canal {CANAL}")

    mes = at.selectbox(key="metas_mes").value
    print(f"         mes de prueba: {mes}")

    for nivel in ["Canal", "Proveedor / línea", "Vendedor"]:
        at.radio(key="metas_nivel_seg").set_value(nivel)
        at.run()
        excepciones(at, f"seguimiento de {CANAL} abierto por {nivel}")

    # --- 1. Objetivo total del canal ---------------------------------------
    ni = [n for n in at.number_input
          if n.key and n.key.startswith("meta_canal_")]
    chk(bool(ni), "existe el input del objetivo del canal")
    if ni:
        ni[0].set_value(25000.0)
        at.run()
        at.button(key="btn_meta_canal").click()
        at.run()
        excepciones(at, "guardar el objetivo del canal")
        chk(abs(dp.total_meta(dp.cargar_metas(), mes, "objetivo", "canal",
                              CANAL) - 25000) < 1,
            "el objetivo del canal quedó persistido")

    # --- 2. Apertura por proveedor -----------------------------------------
    at.button(key="btn_guardar_metas_prov").click()
    at.run()
    excepciones(at, "guardar la apertura por proveedor")
    _tp = dp.total_meta(dp.cargar_metas(), mes, "objetivo", "proveedor", CANAL)
    chk(_tp > 0, f"la apertura por proveedor quedó persistida ({_tp:,.0f} kg)")

    # --- 3. Reparto entre vendedores ---------------------------------------
    at.button(key="btn_guardar_metas_vend").click()
    at.run()
    excepciones(at, "guardar el reparto entre vendedores")

    # --- 4. Presupuesto anual ----------------------------------------------
    at.button(key="btn_guardar_pres").click()
    at.run()
    excepciones(at, "guardar el presupuesto anual")
    _tot_pres = sum(dp.total_meta(dp.cargar_metas(), ms, "presupuesto",
                                  "canal", CANAL) for ms in MESES)
    chk(abs(_tot_pres - 60000) < 1,
        f"el presupuesto de los 12 meses sobrevive el round-trip "
        f"({_tot_pres:,.0f} kg)")

    # --- 5. Controles de consistencia --------------------------------------
    v = dp.validar_metas(dp.cargar_metas(), mes, CANAL)
    f = v[v["control"] == "Σ proveedores vs. objetivo del canal"].iloc[0]
    chk(f["estado"] == "aviso",
        "la validación detecta que la apertura no cierra contra el canal")
    print("        ", f["detalle"])
    p = v[v["control"] == "Objetivo del mes vs. presupuesto anual"]
    chk(len(p) == 1 and p.iloc[0]["estado"] == "info",
        "el objetivo se compara contra el presupuesto como informativo")
    if len(p):
        print("        ", p.iloc[0]["detalle"])

    # --- 6. Historial -------------------------------------------------------
    ov = dp.metas_original_vs_vigente(mes, "objetivo", "canal", CANAL)
    chk(ov is not None and ov["n_cargas"] >= 1,
        "el historial registró la carga del objetivo")

    # Ya no se escribe más: se restauran los datos reales antes del último
    # render, así un corte del proceso no deja el parquet con datos de prueba.
    restaurar()

    # --- 7. Re-render después de restaurar ---------------------------------
    at.run()
    excepciones(at, "la solapa se re-renderiza sin romperse")

finally:
    restaurar()

print("=" * 60)
if fallas:
    print(f"{fallas} falla(s)")
    sys.exit(1)
print("smoke test OK")
