from __future__ import annotations
import os
import logging
import requests
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BREVO_KEY = os.environ["BREVO_KEY"]
OPENAI_KEY = os.environ["OPENAI_KEY"]

BREVO_LIST_ID = int(os.environ.get("BREVO_LIST_ID", "2"))
BREVO_SENDER_NAME = os.environ.get("BREVO_SENDER_NAME", "Reynolds Propiedades")
BREVO_SENDER_EMAIL = os.environ.get("BREVO_SENDER_EMAIL", "info@reynoldspropiedades.com.ar")

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

BREVO_HEADERS = {
    "api-key": BREVO_KEY,
    "Content-Type": "application/json",
}


def clean_bounced_contacts():
    """Fetch hard bounces from Brevo and mark them inactive in Supabase contacts."""
    offset = 0
    limit = 100
    bounced_emails = []
    while True:
        resp = requests.get(
            "https://api.brevo.com/v3/contacts",
            headers=BREVO_HEADERS,
            params={"limit": limit, "offset": offset, "sort": "desc"},
            timeout=60,
        )
        if not resp.ok:
            log.error("Brevo contacts error: %s", resp.text)
            break
        data = resp.json()
        contacts = data.get("contacts", [])
        for c in contacts:
            if c.get("emailBlacklisted") or c.get("smsBlacklisted"):
                email = c.get("email")
                if email:
                    bounced_emails.append(email)
        if len(contacts) < limit:
            break
        offset += limit

    if not bounced_emails:
        log.info("No bounced contacts found")
        return

    log.info("Found %d blacklisted/bounced contacts in Brevo", len(bounced_emails))

    # Mark as inactive in Supabase contacts in batches of 50
    batch_size = 50
    total_cleaned = 0
    for i in range(0, len(bounced_emails), batch_size):
        batch = bounced_emails[i:i + batch_size]
        emails_filter = "(" + ",".join(f'"{e}"' for e in batch) + ")"
        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/contacts?email=in.{emails_filter}",
            headers={**SUPABASE_HEADERS, "Content-Type": "application/json"},
            json={"activo": False},
            timeout=15,
        )
        if r.ok:
            total_cleaned += len(batch)
        else:
            log.error("Supabase patch error: %s", r.text)

    log.info("Marked %d bounced contacts as inactive in Supabase", total_cleaned)


def ai_generate(prompt: str) -> str:
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"},
        json={
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.8,
            "max_tokens": 300,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def generate_novedades_copy(properties: list[dict]) -> tuple[str, str]:
    props_desc = "\n".join(
        f"- {p.get('tipo_propiedad','Propiedad')} en {p.get('barrio','')}, {int(p.get('ambientes') or 0)} amb, {'USD ' + str(int(p.get('precio_venta') or p.get('precio_alquiler') or 0)) if (p.get('precio_venta') or p.get('precio_alquiler')) else 'precio a consultar'}"
        for p in properties
    )
    prompt = f"""Sos el equipo de comunicación de Reynolds Propiedades, una inmobiliaria premium de zona norte de Buenos Aires.
Generá un asunto de email y una frase de introducción para una campaña de propiedades destacadas.

Propiedades de esta semana:
{props_desc}

El tono debe ser: cercano, entusiasta, profesional. Sin clichés. Variá el enfoque cada vez (puede ser urgencia, oportunidad, estilo de vida, inversión, etc.).

Respondé SOLO en este formato (sin explicaciones):
ASUNTO: [asunto del email, máx 60 caracteres]
INTRO: [frase introductoria para el cuerpo del email, 1-2 oraciones, máx 120 caracteres]"""

    result = ai_generate(prompt)
    lines = {line.split(":")[0].strip(): ":".join(line.split(":")[1:]).strip() for line in result.splitlines() if ":" in line}
    subject = lines.get("ASUNTO", "Las propiedades más destacadas de esta semana 🏡")
    intro = lines.get("INTRO", "Seleccionamos las mejores oportunidades para que encuentres tu próxima propiedad.")
    return subject, intro


def generate_newsletter_tip() -> tuple[str, str]:
    prompt = """Sos el equipo editorial de Reynolds Propiedades, inmobiliaria premium de zona norte de Buenos Aires.
Generá el contenido para la sección "Consejo del mes" del newsletter mensual.

El consejo debe ser práctico, relevante para compradores/vendedores/inversores en el mercado inmobiliario argentino actual.
Variá el tema cada vez: puede ser sobre créditos hipotecarios, momento para comprar/vender, cómo tasar, negociación, documentación, etc.

Respondé SOLO en este formato:
TITULO: [título del consejo, máx 60 caracteres]
CUERPO: [desarrollo del consejo, 2-3 oraciones, máx 250 caracteres]"""

    result = ai_generate(prompt)
    lines = {line.split(":")[0].strip(): ":".join(line.split(":")[1:]).strip() for line in result.splitlines() if ":" in line}
    title = lines.get("TITULO", "El momento de comprar es ahora")
    body = lines.get("CUERPO", "El mercado de zona norte sigue en alza. Consultá con un asesor antes de decidir.")
    return title, body


def get_new_properties(limit: int = 3) -> list[dict]:
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/properties",
        headers=SUPABASE_HEADERS,
        params={
            "activa": "eq.true",
            "select": "ref,barrio,direccion,tipo_propiedad,ambientes,dormitorios,total_construido,cocheras,precio_venta,precio_alquiler,moneda,public_url",
            "order": "created_at.desc",
            "limit": limit,
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def property_card(p: dict, accent: str = "#C0392B") -> str:
    barrio = p.get("barrio") or ""
    direccion = p.get("direccion") or ""
    tipo = p.get("tipo_propiedad") or ""
    amb = p.get("ambientes") or ""
    dorm = p.get("dormitorios") or ""
    m2 = p.get("total_construido") or ""
    coch = p.get("cocheras") or ""
    moneda = p.get("moneda") or "USD"

    if p.get("precio_venta"):
        precio = f"{moneda} {int(p['precio_venta']):,}".replace(",", ".")
    elif p.get("precio_alquiler"):
        precio = f"{moneda} {int(p['precio_alquiler']):,}/mes".replace(",", ".")
    else:
        precio = "Consultar"

    url = p.get("public_url") or "https://reynoldspropiedades.com.ar"

    detalles = []
    if amb:
        detalles.append(f"🚪 {int(amb)} amb")
    if dorm:
        detalles.append(f"🛏️ {int(dorm)} dorm")
    if m2:
        detalles.append(f"📐 {int(m2)} m²")
    if coch:
        detalles.append(f"🚗 {int(coch)} coch")
    detalles_str = " &nbsp;·&nbsp; ".join(detalles)

    return f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;border-radius:10px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.07);">
      <tr>
        <td style="background:#f8f9fc;border-left:4px solid {accent};padding:22px 24px;">
          <p style="margin:0 0 4px;font-size:10px;font-weight:700;color:#C0392B;letter-spacing:2px;text-transform:uppercase;">{barrio}</p>
          <p style="margin:0 0 10px;font-size:18px;font-weight:700;color:#1B3A6B;line-height:1.3;">{tipo} · {direccion}</p>
          <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:14px;">
            <tr><td style="font-size:13px;color:#777777;">{detalles_str}</td></tr>
          </table>
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr>
              <td><span style="font-size:24px;font-weight:800;color:#1B3A6B;">{precio}</span></td>
              <td align="right">
                <a href="{url}" style="display:inline-block;padding:10px 20px;background:#C0392B;color:#ffffff;font-size:12px;font-weight:700;text-decoration:none;border-radius:6px;">Ver propiedad →</a>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>"""


def property_row(p: dict) -> str:
    barrio = p.get("barrio") or ""
    direccion = p.get("direccion") or ""
    tipo = p.get("tipo_propiedad") or ""
    amb = p.get("ambientes") or ""
    dorm = p.get("dormitorios") or ""
    m2 = p.get("total_construido") or ""
    moneda = p.get("moneda") or "USD"

    if p.get("precio_venta"):
        precio = f"{moneda} {int(p['precio_venta']):,}".replace(",", ".")
    elif p.get("precio_alquiler"):
        precio = f"{moneda} {int(p['precio_alquiler']):,}/mes".replace(",", ".")
    else:
        precio = "Consultar"

    url = p.get("public_url") or "https://reynoldspropiedades.com.ar"
    detalles = " &nbsp;·&nbsp; ".join(filter(None, [
        f"🚪 {int(amb)} amb" if amb else "",
        f"🛏️ {int(dorm)} dorm" if dorm else "",
        f"📐 {int(m2)} m²" if m2 else "",
    ]))

    return f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:14px;border:1px solid #eaeaea;border-radius:10px;overflow:hidden;">
      <tr>
        <td style="padding:18px 20px;">
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr>
              <td valign="middle">
                <p style="margin:0 0 3px;font-size:10px;font-weight:700;color:#C0392B;letter-spacing:2px;text-transform:uppercase;">{barrio}</p>
                <p style="margin:0 0 6px;font-size:15px;font-weight:700;color:#1B3A6B;">{tipo} · {direccion}</p>
                <p style="margin:0;font-size:12px;color:#888888;">{detalles}</p>
              </td>
              <td align="right" valign="middle" style="padding-left:16px;white-space:nowrap;">
                <p style="margin:0 0 8px;font-size:16px;font-weight:800;color:#1B3A6B;">{precio}</p>
                <a href="{url}" style="display:inline-block;padding:8px 16px;background:#C0392B;color:#ffffff;font-size:11px;font-weight:700;text-decoration:none;border-radius:6px;">Ver →</a>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>"""


def build_novedades_html(properties: list[dict], intro: str = "") -> str:
    cards = "".join(property_card(p, "#C0392B" if i % 2 == 0 else "#1B3A6B") for i, p in enumerate(properties))
    if not intro:
        intro = "Seleccionamos las mejores oportunidades para que encuentres tu próxima propiedad."
    return f"""<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0;padding:0;background-color:#ECEEF2;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background-color:#ECEEF2;">
  <tr><td align="center" style="padding:32px 10px;">
    <table width="580" cellpadding="0" cellspacing="0" style="max-width:580px;width:100%;">
      <tr><td style="padding-bottom:24px;text-align:center;">
        <img src="https://d1v2p1s05qqabi.cloudfront.net/sites/38/media/163287410792.jpeg?v=18" alt="Reynolds Propiedades" width="130" style="display:block;margin:0 auto;border-radius:6px;">
      </td></tr>
      <tr><td style="background:linear-gradient(135deg,#1B3A6B 0%,#0f2347 100%);border-radius:12px 12px 0 0;padding:48px 40px 40px;text-align:center;">
        <p style="margin:0 0 8px;font-size:12px;font-weight:700;color:#C0392B;letter-spacing:3px;text-transform:uppercase;">Esta semana</p>
        <h1 style="margin:0 0 16px;font-size:32px;font-weight:800;color:#ffffff;line-height:1.2;">Propiedades<br>que no podés perder</h1>
        <p style="margin:0 auto;font-size:15px;color:#8aa8d4;line-height:1.7;max-width:400px;">{intro}</p>
      </td></tr>
      <tr><td style="background:#ffffff;border-radius:0 0 12px 12px;padding:32px 40px 40px;">
        {cards}
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr><td style="text-align:center;">
            <p style="margin:0 0 20px;font-size:14px;color:#888888;">¿Buscás algo diferente? Tenemos más propiedades disponibles.</p>
            <a href="https://reynoldspropiedades.com.ar" style="display:inline-block;padding:15px 36px;background:#1B3A6B;color:#ffffff;font-size:14px;font-weight:700;text-decoration:none;border-radius:8px;">Ver todas las propiedades</a>
          </td></tr>
        </table>
      </td></tr>
      <tr><td style="padding:28px 0;text-align:center;">
        <p style="margin:0 0 4px;font-size:13px;font-weight:600;color:#444444;">Equipo Reynolds Propiedades</p>
        <p style="margin:0 0 16px;font-size:12px;color:#999999;"><a href="https://reynoldspropiedades.com.ar" style="color:#C0392B;text-decoration:none;">reynoldspropiedades.com.ar</a></p>
        <p style="margin:0;font-size:11px;color:#bbbbbb;line-height:1.7;">Recibís este email porque estás en nuestra base de contactos.<br><a href="{{{{unsubscribe}}}}" style="color:#bbbbbb;text-decoration:underline;">Cancelar suscripción</a></p>
      </td></tr>
    </table>
  </td></tr>
</table>
</body></html>"""


def build_newsletter_html(properties: list[dict], tip_title: str = "", tip_body: str = "") -> str:
    rows = "".join(property_row(p) for p in properties[:2])
    mes = datetime.now().strftime("%B %Y").capitalize()
    path = os.path.join(os.path.dirname(__file__), "templates", "reynolds-email-newsletter.html")
    html = open(path, encoding="utf-8").read()
    # Replace the two hardcoded property tables with dynamic ones
    import re
    html = re.sub(
        r'<!-- PROPIEDAD_ROWS -->.*?<!-- /PROPIEDAD_ROWS -->',
        rows,
        html,
        flags=re.DOTALL,
    )
    # Replace static property blocks between the section header and the "Ver todas" link
    html = re.sub(
        r'(<p[^>]*>02 — Propiedades destacadas</p>\s*)(.*?)(\s*<table[^>]*>\s*<tr>\s*<td[^>]*>\s*<a href[^>]*>Ver las)',
        lambda m: m.group(1) + rows + m.group(3),
        html,
        flags=re.DOTALL,
    )
    if tip_title:
        import re as _re
        html = _re.sub(
            r'(<p[^>]*>💡</p>\s*<p[^>]*font-size:17px[^>]*>)(.*?)(</p>)',
            lambda m: m.group(1) + tip_title + m.group(3),
            html, flags=_re.DOTALL
        )
        html = _re.sub(
            r'(Las hipotecas crecieron.*?subiendo ahora\.)',
            tip_body,
            html, flags=_re.DOTALL
        )
    return html.replace("Abril 2025", mes)


def create_brevo_draft(name: str, subject: str, html: str, scheduled_at: str | None = None) -> dict:
    payload = {
        "name": name,
        "subject": subject,
        "sender": {"name": BREVO_SENDER_NAME, "email": BREVO_SENDER_EMAIL},
        "type": "classic",
        "htmlContent": html,
        "recipients": {"listIds": [BREVO_LIST_ID]},
    }
    if scheduled_at:
        payload["scheduledAt"] = scheduled_at
    resp = requests.post(
        "https://api.brevo.com/v3/emailCampaigns",
        headers=BREVO_HEADERS,
        json=payload,
        timeout=15,
    )
    if not resp.ok:
        log.error("Brevo error %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()
    return resp.json()


def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    today = datetime.now().strftime("%d/%m/%Y")
    # Schedule: novedades Monday, newsletter Wednesday, leads Friday at 9am
    now = datetime.now()
    # Find next Monday
    days_to_monday = (7 - now.weekday()) % 7 or 7
    monday = now.replace(hour=9, minute=0, second=0, microsecond=0) + timedelta(days=days_to_monday)
    wednesday = monday + timedelta(days=2)
    friday = monday + timedelta(days=4)
    fmt = "%Y-%m-%dT%H:%M:%S+00:00"

    # Limpiar rebotes de Brevo en Supabase
    try:
        clean_bounced_contacts()
    except Exception:
        log.exception("Error limpiando rebotes")

    # 1. Novedades con IA
    props = get_new_properties(limit=3)
    if props:
        try:
            subject, intro = generate_novedades_copy(props)
            log.info("IA generó asunto: %s", subject)
        except Exception:
            log.exception("Error generando copy con IA, usando defaults")
            subject = "Las propiedades más destacadas de esta semana 🏡"
            intro = ""
        html = build_novedades_html(props, intro)
        r = create_brevo_draft(name=f"Novedades {today}", subject=subject, html=html, scheduled_at=monday.strftime(fmt))
        log.info("Campaña novedades creada: id=%s, programada: %s", r.get("id"), monday.strftime(fmt))
    else:
        log.warning("Sin propiedades para campaña de novedades")

    # 2. Newsletter con consejo de IA
    props_nl = get_new_properties(limit=2)
    try:
        tip_title, tip_body = generate_newsletter_tip()
        log.info("IA generó consejo: %s", tip_title)
    except Exception:
        log.exception("Error generando consejo con IA")
        tip_title, tip_body = "El momento de comprar es ahora", "El mercado de zona norte sigue en alza."
    newsletter_html = build_newsletter_html(props_nl, tip_title, tip_body)
    r = create_brevo_draft(
        name=f"Newsletter {today}",
        subject=f"El mercado inmobiliario · {datetime.now().strftime('%B %Y').capitalize()} 📊",
        html=newsletter_html,
        scheduled_at=wednesday.strftime(fmt),
    )
    log.info("Campaña newsletter creada: id=%s, programada: %s", r.get("id"), wednesday.strftime(fmt))

    # 3. Leads (estático)
    with open(os.path.join(os.path.dirname(__file__), "templates", "reynolds-email-vender-alquilar.html"), encoding="utf-8") as f:
        leads_html = f.read()
    r = create_brevo_draft(
        name=f"Captación leads {today}",
        subject="¿Querés vender o alquilar tu propiedad? 🏠",
        html=leads_html,
        scheduled_at=friday.strftime(fmt),
    )
    log.info("Campaña leads creada: id=%s, programada: %s", r.get("id"), friday.strftime(fmt))


if __name__ == "__main__":
    run()
