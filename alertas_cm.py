"""
alertas_cm.py — Alerta por mail cuando el CM% cae debajo del umbral.

Revisa el mes en curso (acumulado) en mart.serie_mensual de Supabase, a tres
niveles: canal, canal/subcanal y vendedor. Si alguno queda con CM% < UMBRAL
(y factura al menos VENTA_MINIMA, para no alertar por ruido), manda un mail.

Para no mandar lo mismo dos veces por día, guarda en alertas_estado.json lo
último que avisó en el mes: solo manda mail cuando aparece un caso NUEVO bajo
el umbral. El mail igual lista todos los casos vigentes, marcando los nuevos.

Configuración en .streamlit/secrets.toml (mismo archivo que usa el panel):

    [alertas]
    smtp_host = "smtp.gmail.com"
    smtp_port = 587
    smtp_user = "cuenta-que-envia@..."
    smtp_password = "contraseña de aplicación"
    destinatarios = ["jmlopeza@dinamicaeye.com", "tyazlle@dinamicaeye.com", "jcsoria@dinamicaeye.com"]
    umbral_cm = 0.10        # opcional, default 10 %
    venta_minima = 1000000  # opcional, $ neto del mes; default 1.000.000

Uso:
    python alertas_cm.py            # revisa y manda si hay casos nuevos
    python alertas_cm.py --prueba   # imprime lo que mandaría, no manda ni guarda estado
    python alertas_cm.py --forzar   # manda aunque no haya casos nuevos
"""
import argparse
import json
import os
import smtplib
import sys
import tomllib
from datetime import datetime
from email.message import EmailMessage
from zoneinfo import ZoneInfo

import fuente_datos

AQUI = os.path.dirname(os.path.abspath(__file__))
RUTA_SECRETS = os.path.join(AQUI, ".streamlit", "secrets.toml")
RUTA_ESTADO = os.path.join(AQUI, "alertas_estado.json")
TZ = ZoneInfo("America/Argentina/Buenos_Aires")

SQL = """
with b as (select * from mart.serie_mensual where anio_mes = %(mes)s)
select 'Canal', "dsCanalMkt", sum("subtotalNeto"), sum(cm)
  from b group by "dsCanalMkt"
union all
select 'Subcanal', "dsCanalMkt" || ' / ' || "dsSubcanalMKT", sum("subtotalNeto"), sum(cm)
  from b group by "dsCanalMkt", "dsSubcanalMKT"
union all
select 'Vendedor', "dsVendedor", sum("subtotalNeto"), sum(cm)
  from b group by "dsVendedor"
"""

ORDEN_NIVEL = {"Canal": 0, "Subcanal": 1, "Vendedor": 2}


def _ars(x):
    return "$ " + f"{x:,.0f}".replace(",", ".")


def _pct(x):
    return f"{100 * x:.1f} %".replace(".", ",")


def cargar_config():
    with open(RUTA_SECRETS, "rb") as f:
        secrets = tomllib.load(f)
    if "alertas" not in secrets:
        sys.exit("Falta la sección [alertas] en .streamlit/secrets.toml")
    return secrets, secrets["alertas"]


def buscar_casos(conninfo, mes, umbral, venta_minima):
    with fuente_datos._conectar(conninfo) as con, con.cursor() as cur:
        cur.execute(SQL, {"mes": mes})
        filas = cur.fetchall()
    casos = []
    for nivel, nombre, venta, cm in filas:
        venta = float(venta or 0)
        cm = float(cm or 0)
        if venta < venta_minima:
            continue
        pct = cm / venta
        if pct < umbral:
            casos.append({"nivel": nivel, "nombre": nombre or "(sin dato)",
                          "venta": venta, "cm": cm, "pct": pct})
    casos.sort(key=lambda c: (ORDEN_NIVEL[c["nivel"]], c["pct"]))
    return casos


def leer_estado():
    try:
        with open(RUTA_ESTADO, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def guardar_estado(mes, claves):
    with open(RUTA_ESTADO, "w", encoding="utf-8") as f:
        json.dump({"mes": mes, "avisados": sorted(claves)}, f, ensure_ascii=False, indent=1)


def armar_mail(casos, nuevos, mes, umbral, ahora):
    asunto = (f"[Panel Gaven] {len(casos)} caso(s) con CM debajo de "
              f"{_pct(umbral)} — {mes}")

    lineas = [f"Mes {mes} (acumulado al {ahora:%d/%m %H:%M}). "
              f"Casos con CM% debajo de {_pct(umbral)}:", ""]
    for c in casos:
        marca = " (NUEVO)" if (c["nivel"], c["nombre"]) in nuevos else ""
        lineas.append(f"- {c['nivel']}: {c['nombre']} — CM {_pct(c['pct'])} "
                      f"sobre {_ars(c['venta'])}{marca}")
    texto = "\n".join(lineas)

    filas = "".join(
        f"<tr style='{'background:#fff3cd' if (c['nivel'], c['nombre']) in nuevos else ''}'>"
        f"<td>{c['nivel']}</td><td>{c['nombre']}</td>"
        f"<td style='text-align:right'>{_ars(c['venta'])}</td>"
        f"<td style='text-align:right'>{_ars(c['cm'])}</td>"
        f"<td style='text-align:right;color:{'#b00020' if c['pct'] < 0 else '#333'}'>"
        f"<b>{_pct(c['pct'])}</b></td></tr>"
        for c in casos)
    html = f"""
<p>Mes <b>{mes}</b> (acumulado al {ahora:%d/%m %H:%M}).
Casos con CM% debajo de <b>{_pct(umbral)}</b>. En amarillo, los nuevos desde el último aviso.</p>
<table cellpadding="6" cellspacing="0" border="1" style="border-collapse:collapse;font-family:Arial;font-size:13px">
<tr style="background:#eee"><th>Nivel</th><th>Nombre</th><th>Venta neta</th><th>CM $</th><th>CM %</th></tr>
{filas}
</table>
<p style="color:#777;font-size:12px">A principio de mes el CM puede salir bajo si todavía
no están cargados los acuerdos de costo. Aviso automático del panel de Gaven.</p>
"""
    return asunto, texto, html


def enviar(cfg, asunto, texto, html):
    msg = EmailMessage()
    msg["Subject"] = asunto
    msg["From"] = cfg["smtp_user"]
    msg["To"] = ", ".join(cfg["destinatarios"])
    msg.set_content(texto)
    msg.add_alternative(html, subtype="html")
    with smtplib.SMTP(cfg.get("smtp_host", "smtp.gmail.com"),
                      int(cfg.get("smtp_port", 587)), timeout=30) as s:
        s.starttls()
        s.login(cfg["smtp_user"], cfg["smtp_password"])
        s.send_message(msg)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prueba", action="store_true", help="no manda ni guarda estado")
    ap.add_argument("--forzar", action="store_true", help="manda aunque no haya casos nuevos")
    ap.add_argument("--mes", help="YYYY-MM (default: mes en curso)")
    args = ap.parse_args()

    secrets, cfg = cargar_config()
    umbral = float(cfg.get("umbral_cm", 0.10))
    venta_minima = float(cfg.get("venta_minima", 1_000_000))
    ahora = datetime.now(TZ)
    mes = args.mes or ahora.strftime("%Y-%m")

    conninfo = fuente_datos.resolver_conninfo(secrets)
    casos = buscar_casos(conninfo, mes, umbral, venta_minima)

    estado = leer_estado()
    ya_avisados = set(map(tuple, estado.get("avisados", []))) if estado.get("mes") == mes else set()
    actuales = {(c["nivel"], c["nombre"]) for c in casos}
    nuevos = actuales - ya_avisados

    print(f"{ahora:%Y-%m-%d %H:%M} mes={mes} casos={len(casos)} nuevos={len(nuevos)}")
    if not casos:
        if not args.prueba:
            guardar_estado(mes, set())  # si se recuperan, se puede volver a avisar
        return
    if not nuevos and not args.forzar:
        return

    asunto, texto, html = armar_mail(casos, nuevos, mes, umbral, ahora)
    if args.prueba:
        print(asunto)
        print(texto)
        return
    enviar(cfg, asunto, texto, html)
    guardar_estado(mes, actuales)
    print("Mail enviado a", ", ".join(cfg["destinatarios"]))


if __name__ == "__main__":
    main()
