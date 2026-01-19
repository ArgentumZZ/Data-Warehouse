import utilities.logging_manager as lg
import smtplib
from email.mime.text import MIMEText

class EmailManager:

    def __init__(self, factory: 'ScriptFactory') -> None:

        self.factory = factory
        self.settings = factory.settings

        # from factory (based on environment)
        self.recipients_admin = self.settings.list_recipients_admin
        self.recipients_business = self.settings.list_recipients_business
        self.recipients_error = self.settings.list_recipients_error

        # To send or not to send e-mails
        self.is_admin_email_alert_enabled = self.settings.is_admin_email_enabled
        self.is_business_email_alert_enabled = self.settings.is_business_email_enabled
        self.is_error_email_alert_enabled = self.settings.is_error_email_enabled

        # SMTP config (host, port, username, password, from_addr), probably in config_file.cfg
        self.smtp_config = self.settings.smtp_config

        # prepared emails payloads
        # Prepared email payloads
        self.prepared_success_email = None
        self.prepared_error_email = None


    def prepare_mails(self, tasks):
        """
        Build the email body from:
        - factory.info (general script information)
        - tasks lists (each task with its parameters) - returned by factory.init_tasks() or integrate in run_script.py
        """

        lg.info("Preparing email content")
        info = self.factory.info

        # Build the mail structure
        # General info block
        general_info_html = """
        <h2>General Script Information </h2>
        <table border="1" cellspacing="0" cellpadding="6" style="border-collapse:collapse;">
        """
        for key, value in info.items():
            general_info_html += f"""
            <tr>
                <td><b>{key}</b></td>
                <td>{value}</td>
            </tr>
            """
        general_info_html += "</table><br>"

        # Task details block (based on init_tasks_definitions)
        task_html = """
        <h2>Task Definitions and Parameters</h2>
        <table border="1" cellspacing="0" cellpadding="6" style="border-collapse:collapse;">
        <tr>
            <th>Task Name</th>
            <th>Description</th>
            <th>Enabled</th>
            <th>Retries</th>
            <th>Depends On</th>
            <th>Parameters</th>
        </tr>
        """

        # need to display the logs in the e-mail too
        for t in tasks:
            func = t["func"]

            # extract parameters from partial if available
            if hasattr(func, "keywords") and func.keywords is not None:
                params_repr = ", ".join(f"{k}={v!r}" for k, v in func.keywords.items())
            else:
                params_repr = ""

            task_html += f"""
            <tr>
                <td>{t['task_name']}</td>
                <td>{t.get('description', '')}</td>
                <td>{t.get('enabled', True)}</td>
                <td>{t.get('retries', 0)}</td>
                <td>{t.get('depends_on', '')}</td>
                <td>{params_repr}</td>
            </tr>
            """
        task_html += "</table>"

        # Final HTML body structure
        html_body = f"""
        <html>
        <body>
        {general_info_html}
        {task_html}
        </body>
        </html>
        """

        # store prepared e-mails
        self.prepared_success_email = {
            "to"        : self.recipients_business,
            "subject"   : f"{info['script_name']} ETL Run",
            "body"      : html_body
        }

        self.prepared_error_email = {
            "to"        : self.recipients_error,
            "subject"   : f"{info['script_name']} ERROR Report",
            "body"      : html_body
        }


    def send_mails(self, is_error=False):
        """
        1. Send the prepared email.
        2. is_error=True → send to error recipients with error subject.
       """

        # grab the error in run_script.py to determine behavior
        # self.is_business_email_alert_enabled
        if not self.settings.send_mail_report:
            lg.info("Email sending disabled in settings.")
            return

        if self.prepared_success_email is None:
            lg.error("prepare_mails() must be called before send_emails()")
            return

        # if error is True -> send self.prepared_error_mail
        # if error is False -> send self.prepared_success_email
        email_to_send = self.prepared_error_email if is_error else self.prepared_success_email

        # run the smtp_send function
        self.smtp_send(to=email_to_send['to'],
                       subject=email_to_send['subject'],
                       body=email_to_send['body']
                       )

    def smtp_send(self, to, subject, body):
        lg.info(f"Sending email to: {to}")
        lg.info(f"Subject: {subject}")

        # Create a MIME email object with the given HTML body
        # MIMEText handles proper encoding and marks the content type as HTML.
        msg = MIMEText(body, "html")

        # Set the email subject header.
        msg["Subject"] = subject

        # Set the sender address that will appear in the email client.
        # This must match the authenticated SMTP account for Gmail.
        msg["From"] = self.smtp_config['from_addr']

        # Join the list of recipient email addresses into a single comma-separated string.
        # This is only for display in the email header; the actual delivery uses the 'to' list in sendmail() below
        msg["To"] = ", ".join(to)

        # Establish a connection to the SMTP server (e.g., smtp.gmail.com:587)
        with smtplib.SMTP(self.smtp_config['host']. self.smtp_config['port']) as server:
            # Upgrade the connection to a secure encrypted TLS channel.
            # Required by Gmail and most modern SMTP servers
            server.starttls()

            # Authenticate using the SMTP username and password.
            # For Gmail: username = my Gmail address, password = App Password.
            server.login(self.smtp_config['username'],
                         self.smtp_config['password'])

            # Send the email message over the authenticated, encrypted SMTP session.
            server.sendmail(self.smtp_config['from_addr'],  # Sender email address
                            to, 				            # List of recipient email addresses
                            msg.as_string()		            # Full MIME email (headers + HTML body)
                            )

        lg.info("Email sent successfully.")