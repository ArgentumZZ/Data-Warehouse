# import libraries
import smtplib, os, configparser
from typing import Dict
from email.mime.text import MIMEText
from datetime import datetime

# import custom libraries
import utilities.logging_manager as lg
from utilities.config_utils import load_smtp_config

class EmailManager:

    def __init__(self, factory: 'ScriptFactory') -> None:

        self.factory = factory

        # Lists of recipients that change based on PROD/DEV environment.
        self.recipients_admin = self.factory.list_recipients_admin
        self.recipients_business = self.factory.list_recipients_business
        self.recipients_error = self.factory.list_recipients_error

        # Determine if e-mail alerts are enabled.
        self.is_admin_email_alert_enabled = self.factory.is_admin_email_enabled
        self.is_business_email_alert_enabled = self.factory.is_business_email_enabled
        self.is_error_email_alert_enabled = self.factory.is_error_email_enabled

        # SMTP config loaded by the internal helper function
        self.smtp_config = None

        # Define prepared emails payloads
        self.prepared_success_email = None
        self.prepared_error_email = None

        # Initialize the dynamic HTML content
        self.task_count = 0
        self.html_tasks_rows = ""
        self.html_logs_blocks = ""  # Initialize the log block accumulator


    def add_task_result_to_email(self,
                                 task: dict,
                                 status: str,
                                 error_msg: str = "") -> None:
        """
        1. Appends a formatted HTML table row to the internal task log string.

        2. This method is called iteratively as each task completes. It applies
        conditional formatting (green for success, red for failure) and
        extracts function parameters in the report.

        Args:
            task: The task configuration dictionary from ScriptFactory.
            status: The execution outcome (e.g., 'SUCCESS', 'FAILED', 'SKIPPED').
            error_msg: The exception message if the task failed. Defaults to "".
        """

        # 1. Extract function keywords/parameters
        func = task["function"]
        params_repr = ""

        # A number that will be displayed next to the task name, increment the counter
        self.task_count += 1

        # Check if the function has 'keywords' attribute
        if hasattr(func, "keywords") and func.keywords:
            # Use <br/> to ensure that each pair starts on a new line in the HTML table
            params_repr = "<br/>".join(f"{k}={v!r}" for k, v in func.keywords.items())

        # 2. Determine row styling and status text based on outcome (green/red)
        color = "#d4edda" if status == "SUCCESS" else "#f8d7da"

        # If failed, add the 'FAILED' tag to the error message
        status_text = status if status == "SUCCESS" else f"FAILED: {error_msg}"

        # 3. Append the formatted row to the instance variable self.html_tasks_rows
        self.html_tasks_rows += f"""
            <tr style="background-color: {color};">
                <td style="text-align: center;">{self.task_count}</td>
                <td>{task['task_name']}</td>
                <td>{task.get('description', '')}</td>
                <td>{task.get("is_enabled")}</td>
                <td>{task.get("retries")}</td>
                <td>{task.get("depends_on")}</td>
                <td>{status_text}</td>
                <td style="font-size: 10px;">{params_repr}</td>
            </tr>
        """

    def add_log_block_to_email(self,
                               task_name: str,
                               logs: str) -> None:
        """
        Append a formatted block of logs.
        """

        display_logs = logs.strip() if (logs and logs.strip()) else "No logs captured for this task."

        # Keep the HTML tags for the container indented,
        # but the content and its immediate parent MUST be at the start of the line.
        self.html_logs_blocks += f"""
                <div style="border: 1px solid #dddddd; border-bottom: none; overflow: hidden;">
                <div style="background-color: #343a40; color: #ffffff; padding: 6px 12px; font-family: monospace; font-size: 13px; font-weight: bold;">
                TASK {self.task_count}: {task_name}
                </div>
                <div style="background-color: #f8f9fa; padding: 10px 15px; font-family: 'Consolas', monospace; white-space: pre-wrap; font-size: 12px; color: #333333;">{display_logs}</div>
                </div>
                """

    def prepare_mails(self, log_content: str = "") -> None:
        """
        1. Finalize the HTML structure and prepare email payloads for delivery.

        2. This method aggregates the general script metadata and the accumulated
        task execution rows into a single HTML document. It then stores
        these as dictionaries in 'prepared_success_email' and 'prepared_error_email'.

        3. Read the log content of the run and display it in the e-mail alert.
        Raises:
            KeyError: If 'script_name' is missing from factory.info.
        """

        lg.info("Finalizing email content structure")
        # 1. Access script metadata from the factory instance
        info = self.factory.info

        # 1. Build the general info header table
        # This section provides context (e.g., Environment, Start Time) at the top of the email
        general_info_html = """
        <h2>General Script Information</h2>
        <table border="1" cellspacing="0" cellpadding="6" style="border-collapse:collapse;">
        """

        # With gray background
        for key, value in info.items():
            general_info_html += f"<tr><td style='background-color: #f2f2f2; width: 30%;'><b>{key}</b></td><td>{value}</td></tr>"
        general_info_html += "</table><br>"

        # 2. Build the task table header
        # Defines the column structure for the execution results appended during the run
        task_table_header = """
        <h2>Task Execution Log</h2>
        <table border="1" cellspacing="0" cellpadding="6" style="border-collapse:collapse; width: 100%;">
        <tr style="background-color: #f2f2f2;">
            <th>#</th>
            <th>Task name</th>
            <th>Description</th>
            <th>Is enabled</th>
            <th>Retries</th>
            <th>Depends on</th>
            <th>Status</th>
            <th>Parameters</th>
        </tr>
        """

        # 3. Use the accumulated blocks instead of a single string
        # Wrap the accumulated blocks in a div that adds the final bottom border
        log_section_html = f"""
                        <hr style="border: 1px solid #eee; margin: 30px 0;">
                        <h2>Technical Log Details</h2>
                        <div style="border-bottom: 1px solid #dddddd;"> 
                            {self.html_logs_blocks if self.html_logs_blocks else "<em>No logs captured.</em>"}
                        </div>
                        """

        # 4. Combine header + accumulated rows + table closing tags
        # This creates the full HTML document string
        html_body = f"""
        <html>
        <body>
            {general_info_html}
            {task_table_header}
            {self.html_tasks_rows}
        </table>
        <br>
            {log_section_html}
        </body>
        </html>
        """

        today = datetime.now().strftime("%Y-%m-%d")

        # 4. Store in payloads for the send_mails() method
        # Success payload: Sent to Business recipients
        self.prepared_success_email = {
            "to": self.recipients_business,
            "subject": f"SUCCESS: {info['script_name']} - [{today}]",
            "body": html_body
        }

        # Error payload: Sent to Error recipients
        self.prepared_error_email = {
            "to": self.recipients_error,
            "subject": f"ERROR: {info['script_name']} - [{today}]",
            "body": html_body
        }

    def send_mails(self, is_error: bool = False) -> None:
        """
        1. Orchestrate the distribution of separate emails to admin, business and error groups.
        2. This method ensures the correct recipients receive the appropriate report based on the script's
        final status and the environment.

        Args:
            is_error: Flag indicating if the script failed. Defaults to False (Success).
        """

        lg.info("Beginning separate email distribution logic")

        # 1. Select the appropriate payload based on the script outcome
        # This determines whether the subject lines will display 'SUCCESS' or 'ERROR'
        email_payload = self.prepared_error_email if is_error else self.prepared_success_email

        # Check that the HTML body has been constructed
        if email_payload is None:
            lg.info("No prepared email found. Call prepare_mails() first.")
            return

        # 2. Admin group
        if self.is_admin_email_alert_enabled:
            lg.info("Sending dedicated email to Admin group.")
            self.smtp_send(
                to=self.recipients_admin,
                subject=f"[ADMIN] {email_payload['subject']}",
                body=email_payload['body']
            )

        # 3. Business group: only send if the script succeeded
        if not is_error and self.is_business_email_alert_enabled:
            lg.info("Sending dedicated email to Business group.")
            self.smtp_send(
                to=self.recipients_business,
                subject=email_payload['subject'],
                body=email_payload['body']
            )

        # 4. Error group: only send if the script failed
        if is_error and self.is_error_email_alert_enabled:
            lg.info("Sending dedicated email to Error group.")
            self.smtp_send(
                to=self.recipients_error,
                subject=email_payload['subject'],
                body=email_payload['body']
            )


    def smtp_send(self, to: list[str], subject: str, body: str) -> None:

        """
        1. Execute the technical transmission of an email via SMTP.
        2. This method handles:
                - the creation of the MIME message
                - establishes a secure TLS connection to the mail server
                - authenticates using stored credentials
                - dispatches the email to the recipient list.

        Args:
            to: A list of recipient email addresses.
            subject: The subject line for the email.
            body: The HTML content for the email body.

        Raises:
            smtplib.SMTPException: If there is an error during connection or authentication.
        """

        self.smtp_config = load_smtp_config()

        lg.info(f"Sending email to: {to}")
        lg.info(f"Subject: {subject}")

        # Create a MIME email object with the given HTML body
        # MIMEText handles proper encoding and marks the content type as HTML.
        msg = MIMEText(body, "html")

        # Set the email subject header.
        msg["Subject"] = subject

        # Set the sender address that will appear in the email client.
        # This must match the authenticated SMTP account for Gmail.
        msg["From"] = self.smtp_config['from_address']

        # Join the list of recipient email addresses into a single comma-separated string.
        # This is only for display in the email header; the actual delivery uses the 'to' list in sendmail() below
        msg["To"] = ", ".join(to)

        # Establish a connection to the SMTP server (e.g., smtp.gmail.com:587)
        with smtplib.SMTP(self.smtp_config['host'], self.smtp_config['port']) as server:
            # Upgrade the connection to a secure encrypted TLS channel.
            # Required by Gmail and most modern SMTP servers
            server.starttls()

            # Authenticate using the SMTP username and password.
            # For Gmail: username = my Gmail address, password = App Password.
            server.login(self.smtp_config['username'],
                         self.smtp_config['password'])

            # Send the email message over the authenticated, encrypted SMTP session.
            server.sendmail(self.smtp_config['from_address'],  # Sender email address
                            to, 				               # List of recipient email addresses
                            msg.as_string()		               # Full MIME email (headers + HTML body)
                            )

        lg.info("Email sent successfully.")
