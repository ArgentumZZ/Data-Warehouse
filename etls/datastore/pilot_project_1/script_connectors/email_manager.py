# email_manager.py

import common.logging as lg


class EmailManager:

    # -------------------------------------------------------
    # Internal helper class: MailInput
    # -------------------------------------------------------
    class MailInput:
        def __init__(self):
            self.to = []
            self.error_to = []
            self.subject = ""
            self.error_subject = ""
            self.templates = []
            self.html_parts = []

        def add_recipients(self, to, error_to, subj, err_subject):
            self.to = to
            self.error_to = error_to
            self.subject = subj
            self.error_subject = err_subject

        def add_template(self, ptp, thefile):
            self.templates.append((ptp, thefile))

        def add_html_part(self, part, template, text):
            self.html_parts.append({
                "part": part,
                "template": template,
                "text": text
            })

        def send(self):
            lg.logger.info(f"Sending email to: {self.to}")
            lg.logger.info(f"Subject: {self.subject}")
            # Here you would integrate your actual mailer logic
            # This is a placeholder for demonstration
            lg.logger.info("Email sent successfully")

    # -------------------------------------------------------
    # Internal helper function: dataframe_to_html
    # -------------------------------------------------------
    @staticmethod
    def dataframe_to_html(df, font_size=11):
        if df is None:
            return "<p>No data available</p>"

        html = df.to_html(index=False)
        return f"<div style='font-size:{font_size}px'>{html}</div>"

    # -------------------------------------------------------
    # EmailManager initialization
    # -------------------------------------------------------
    def __init__(self, factory):
        self.factory = factory
        self.cfg = factory.cfg

        self.recipients_admin = self.cfg.recipients_list_admin
        self.recipients_business = self.cfg.recipients_list_business
        self.recipients_error = self.cfg.error_recipients

        self.mails = []

    # -------------------------------------------------------
    # 1. Prepare email content
    # -------------------------------------------------------
    def prepare_mails(self):
        lg.logger.info("Preparing email content")

        self.mails.clear()
        mail = EmailManager.MailInput()

        mail.add_recipients(
            to=self.recipients_business,
            error_to=self.recipients_error,
            subj=f"{self.cfg.script_name}: {self.factory.worker.run_date}",
            err_subject=f"{self.cfg.script_name} ERROR REPORT"
        )

        mail.add_template(
            ptp="message",
            thefile=["templates", "msgtemplate_j01.html"]
        )

        mail.add_html_part(
            part="body_segment",
            template=["templates", "body_header.html"],
            text="ETL Run Summary"
        )

        df_html = EmailManager.dataframe_to_html(self.factory.worker.df_result)
        mail.add_html_part(
            part="body_segment",
            template=["templates", "body_segment.html"],
            text=df_html
        )

        self.mails.append(mail)

    # -------------------------------------------------------
    # 2. Send emails
    # -------------------------------------------------------
    def send_mails(self):
        lg.logger.info("Sending emails")

        if not self.cfg.send_mail_report:
            lg.logger.info("Email sending disabled in settings")
            return

        for mail in self.mails:
            mail.send()
