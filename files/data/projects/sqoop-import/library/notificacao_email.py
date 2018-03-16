import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


defaults = {
    "smtp_server": "smtp01.b2wdigital.com",
    "sender_address": {"name": "Info Cargas", "email": "no-reply.infocargas@b2wdigital.com"},
    "receivers": [
        {"name": "Gilmar Amaral", "email": "gilmar.amaral@b2wdigital.com"},
        {"name": "Thiago Henrique", "email": "thiago.bento@b2wdigital.com"},
        {"name": "Felipe Naberezny", "email": "felipe.naberezny@b2wdigital.com"},
        {"name": "Armando Yamada", "email": "armando.yamada@b2wdigital.com"}
    ],
    "receivers_format": "{name} <{email}>",
    "banner":
        """Este email e gerado automaticamente, favor nao responder."""
        """Caso necessario, entrar em contato no ramal 3169.

                         \nAtt.,
                         \nInfo Cargas
        \n"""
}

def notificacao(subject, message, log=None, **attrs):
    # This call SMTP configuration
    smtp_conf = defaults.copy()
    smtp_conf.update(attrs)

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = str(subject)
        msg['From'] = smtp_conf["receivers_format"].format(**smtp_conf["sender_address"])
        msg['To'] = ",".join(
            map(
                lambda r: smtp_conf["receivers_format"].format(**smtp_conf["sender_address"]),
                smtp_conf["receivers"]
            )
        )

        part1 = MIMEText(smtp_conf["banner"] + message)
        msg.attach(part1)

        smtp_server = smtplib.SMTP(smtp_conf["smtp_server"])
        smtp_server.sendmail(
            smtp_conf["sender_address"]["email"],
            map(lambda r: r["email"], smtp_conf["receivers"]),   # E-mails only
            msg.as_string()
        )

    except Exception as e:
        if log:
            log.error("Erro ao tentar enviar email de notificacao. %s", str(e))
