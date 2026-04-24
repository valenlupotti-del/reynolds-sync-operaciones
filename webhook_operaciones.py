from __future__ import annotations
import os
import json
import smtplib
import logging
import threading
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from flask import Flask, request, jsonify
import requests
import sync_properties
import create_campaigns

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

GMAIL_USER = os.environ.get("GMAIL_USER", "valenlupotti@gmail.com")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "bnsa axel vpqq hvhr")
GMAIL_FROM_NAME = os.environ.get("GMAIL_FROM_NAME", "Reynolds Propiedades")

TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")

ROLE_TEMPLATE = {
    "comprador":   "email_comprador.html",
    "vendedor":    "email_vendedor.html",
    "inquilino":   "email_inquilino.html",
    "propietario": "email_propietario.html",
    "tasacion":    "email_tasacion.html",
    "tasación":    "email_tasacion.html",
}

TALLY_FIELD_MAP = {
    "Asesor":                           "asesor_nombre",
    "Sucursal":                         "sucursal",
    "Fecha de firma":                   "fecha_firma",
    "Dirección de la propiedad":        "direccion",
    "Tipo de propiedad":                "tipo_propiedad",
    "Tipo de Operación":                "tipo_operacion",
    "Monto en USD":                     "monto",
    "Porcentaje de comisión":           "porcentaje_comision",
    "Monto mensual":                    "monto_mensual",
    "Plazo en meses?":                  "plazo",
    "Valor Tasación":                   "comision",
    "Nombre del cliente":               "cliente_nombre",
    "Email del cliente":                "cliente_email",
    "Rol del cliente":                  "cliente_rol",
    "Nombre de la contraparte":         "contraparte_nombre",
    "Email de la contraparte":          "contraparte_email",
    "Rol de la contraparte":            "contraparte_rol",
    "Observaciones":                    "observaciones",
}


def load_template(role: str) -> str | None:
    filename = ROLE_TEMPLATE.get(role.lower().strip())
    if not filename:
        return None
    path = os.path.join(TEMPLATES_DIR, filename)
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        log.error("Template not found: %s", path)
        return None


def send_email(to_email: str, subject: str, html_body: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{GMAIL_FROM_NAME} <{GMAIL_USER}>"
    msg["To"] = to_email
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, to_email, msg.as_string())
    log.info("Email sent to %s", to_email)


def parse_tally_payload(payload: dict) -> dict:
    data = {}
    fields = payload.get("data", {}).get("fields", [])
    log.info("Tally fields received: %s", [f.get("label") for f in fields])
    for field in fields:
        label = field.get("label", "")
        value = field.get("value")
        options = field.get("options", [])

        # Resolve UUID option IDs to text labels
        if options and value is not None:
            option_map = {o["id"]: o["text"] for o in options if "id" in o and "text" in o}
            if isinstance(value, list):
                value = [option_map.get(v, v) for v in value]
            else:
                value = option_map.get(value, value)

        # Unwrap single-item lists
        if isinstance(value, list) and len(value) == 1:
            value = value[0]

        key = TALLY_FIELD_MAP.get(label)
        if key:
            data[key] = value
    return data


def insert_operacion(data: dict) -> dict | None:
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    row = {k: v for k, v in data.items() if v not in (None, "", [])}
    row["fecha_carga"] = datetime.utcnow().isoformat()

    # Convert numeric strings
    for field in ("monto", "monto_mensual", "plazo", "porcentaje_comision", "comision"):
        if field in row:
            try:
                row[field] = float(str(row[field]).replace(",", "."))
            except (ValueError, TypeError):
                row.pop(field, None)

    # Calculate comision
    tipo = (row.get("tipo_operacion") or "").lower()
    pct = row.get("porcentaje_comision")
    if pct and "alquiler" in tipo:
        mensual = row.get("monto_mensual")
        plazo = row.get("plazo")
        if mensual and plazo:
            row["comision"] = round(mensual * plazo * pct / 100, 2)
    elif pct and "venta" in tipo:
        monto = row.get("monto")
        if monto:
            row["comision"] = round(monto * pct / 100, 2)

    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/operaciones",
        headers=headers,
        json=row,
        timeout=15,
    )
    if not resp.ok:
        log.error("Supabase error %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()
    result = resp.json()
    return result[0] if result else None


def notify_client(name: str, email: str, role: str, asesor: str):
    template = load_template(role)
    if not template:
        log.warning("No template for role '%s', skipping email to %s", role, email)
        return
    html = template.replace("{NOMBRE_CLIENTE}", name).replace("{ASESOR}", asesor)

    role_subjects = {
        "comprador":   "¡Tu nueva propiedad ya es tuya! 🏡",
        "vendedor":    "¡Tu propiedad encontró su dueño! ✨",
        "inquilino":   "¡Tu nuevo hogar te está esperando! 🔑",
        "propietario": "Tu propiedad ya tiene nuevos inquilinos 🤝",
        "tasacion":    "¡Tu tasación está lista! 📊",
        "tasación":    "¡Tu tasación está lista! 📊",
    }
    subject = role_subjects.get(role.lower().strip(), "Reynolds Propiedades — gracias por confiar en nosotros")
    send_email(email, subject, html)


@app.route("/webhook/operaciones", methods=["POST"])
def webhook():
    payload = request.get_json(force=True, silent=True)
    if not payload:
        return jsonify({"error": "invalid payload"}), 400

    log.info("Tally submission received")

    try:
        data = parse_tally_payload(payload)
    except Exception as e:
        log.exception("Error parsing Tally payload")
        return jsonify({"error": str(e)}), 422

    if not data:
        log.warning("Empty data after parsing, raw payload: %s", json.dumps(payload)[:500])
        return jsonify({"error": "no recognizable fields"}), 422

    # Insert into Supabase
    try:
        insert_operacion(data)
        log.info("Operation inserted for %s", data.get("cliente_nombre", "?"))
    except Exception as e:
        log.exception("Supabase insert failed")
        return jsonify({"error": f"DB insert failed: {e}"}), 500

    asesor = data.get("asesor_nombre", "tu asesor")

    # Send email to main client
    cliente_email = data.get("cliente_email", "")
    cliente_nombre = data.get("cliente_nombre", "")
    cliente_rol = data.get("cliente_rol", "")
    if cliente_email and cliente_nombre and cliente_rol:
        try:
            notify_client(cliente_nombre, cliente_email, cliente_rol, asesor)
        except Exception as e:
            log.error("Failed to email client %s: %s", cliente_email, e)

    # Send email to counterpart if present
    contra_email = data.get("contraparte_email", "")
    contra_nombre = data.get("contraparte_nombre", "")
    contra_rol = data.get("contraparte_rol", "")
    if contra_email and contra_nombre and contra_rol:
        try:
            notify_client(contra_nombre, contra_email, contra_rol, asesor)
        except Exception as e:
            log.error("Failed to email counterpart %s: %s", contra_email, e)

    return jsonify({"ok": True}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


def properties_sync_loop():
    while True:
        sync_properties.run()
        time.sleep(3600)  # every hour


def campaigns_loop():
    # Wait 1 hour on startup, then run every Monday at ~9am
    time.sleep(3600)
    while True:
        now = datetime.now()
        if now.weekday() == 0 and now.hour == 9:  # Monday 9am
            try:
                create_campaigns.run()
            except Exception:
                log.exception("Error creating campaigns")
        time.sleep(3600)  # check every hour


if __name__ == "__main__":
    threading.Thread(target=properties_sync_loop, daemon=True).start()
    threading.Thread(target=campaigns_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)
