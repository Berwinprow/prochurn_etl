import smtplib, ssl
from email.mime.text import MIMEText
from airflow.utils.email import get_email_address_list

def send_email_smtp(
    to, subject, html_content, from_email=None, files=None, dryrun=False, **kwargs
):
    sender = from_email or "berwin.rayen@prowesstics.com"
    password = "xretwrckfaffmphu"
    recipients = get_email_address_list(to)

    msg = MIMEText(html_content, "html")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender, password)
        server.sendmail(sender, recipients, msg.as_string())
    print(f"✅ Custom email sent to {recipients}")
