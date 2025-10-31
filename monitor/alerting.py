import asyncio
from email.message import EmailMessage
import json
import logging
import smtplib
import ssl


def send_email(smtp_cfg: dict, subject: str, body: str):
    if not smtp_cfg or not smtp_cfg.get("smtp_server"):
        return
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = smtp_cfg.get("from")
    msg["To"] = smtp_cfg.get("to")
    ctx = ssl.create_default_context()
    with smtplib.SMTP(
        smtp_cfg["smtp_server"], smtp_cfg.get("smtp_port", 587), timeout=10
    ) as s:
        s.starttls(context=ctx)
        if smtp_cfg.get("username"):
            s.login(smtp_cfg["username"], smtp_cfg["password"])
        s.send_message(msg)


async def send_webhook(url: str, payload: dict):
    data = json.dumps(payload)
    proc = await asyncio.create_subprocess_exec(
        "curl",
        "-sS",
        "-X",
        "POST",
        "-H",
        "Content-Type: application/json",
        "-d",
        data,
        url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()
    if proc.returncode != 0:
        logging.warning("webhook post failed")


async def alert(cfg_alert: dict, subject: str, payload: dict):
    if cfg_alert.get("webhook"):
        try:
            await send_webhook(cfg_alert["webhook"], payload)
        except Exception as e:
            logging.exception("webhook error: %s", e)
    if cfg_alert.get("email", {}).get("smtp_server"):
        try:
            send_email(cfg_alert["email"], subject, json.dumps(payload, indent=2))
        except Exception as e:
            logging.exception("email error: %s", e)
