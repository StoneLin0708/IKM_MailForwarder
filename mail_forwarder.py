from smtplib import SMTP
from imap_tools import MailBox, UidRange, AND
import email
import logging
import datetime as dt
from email import policy
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging.handlers import TimedRotatingFileHandler

import json
import os
import signal
import time


class MailForwarder:
    def __init__(self, config, last_send, members, subject_filter, logger):
        self.config = json.load(open(config))
        self.last_send = last_send
        m = json.load(open(members))
        self.users = m['users']
        self.managers = m['managers']
        self.logger = logger
        self.subject_filter = [x.strip()
                               for x in open(subject_filter).readlines()]

    def _is_filtered(self, mail):
        return mail['Subject'] in self.subject_filter

    def _get_last_send(self):
        if not os.path.exists(self.last_send):
            raise Exception('last send not found')
        return int(open(self.last_send).read().strip())

    def _set_last_send(self, value):
        open(self.last_send, 'w').write(str(value))

    def _get_user_emails(self):
        return self.users.values()

    def _get_manager_emails(self):
        return self.users.values()

    def _create_error_mail(self, subject, error_text):
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.config['smtpuser']
        msg['To'] = ','.join(self._get_manager_emails())
        msg.attach(MIMEText(error_text, 'plain'))
        return msg

    def _send_mails_to_manager(self, mail):
        try:
            with SMTP(self.config['host'], port=self.config['smtp_port']) as server:
                server.starttls()
                l = server.login(
                    self.config['smtp_user'], self.config['smtp_pw'])
                ret = server.sendmail(self.config['smtp_user'],
                                      self._get_manager_emails(),
                                      mail.as_string())
                self.logger.error(f'`{mail["subject"]}` sending failed')
                for address, err in ret.items():
                    self.logger.error(f'{address}: {err}')
        except Exception as e:
            self.logger.error(f'`{mail["subject"]}` sending failed : {e}')

    def update(self):
        # at least return the mail with 'last_send' uid
        # if there is new mail, there will be more than
        # one uid
        query = AND(uid=UidRange(self._get_last_send(), '*'))
        mails = None
        try:
            with MailBox(self.config['host'], port=self.config['imap_port'], starttls=True).login(self.config['imap_user'], self.config['imap_pw'], 'INBOX') as mailbox:
                mails = list(map(lambda msg: (msg.uid, email.message_from_bytes(msg.obj.as_bytes(), policy=policy.default)),
                                 mailbox.fetch(query, mark_seen=False)))
        except Exception as e:
            self.logger.error(f'fetch mail failed : {e}')

        if mails is None:
            return

        if len(mails) > 1:
            # ignore the first email which has been sent last time
            self._forward_email(mails[1:])
            # skip failed mails
            self._set_last_send(mails[-1][0])
        else:
            interval = self.config['update_interval']
            print(f'{time.time()}: start count down for {interval} seconds')
            time.sleep(interval)

    def _forward_email(self, mails):
        try:
            with SMTP(self.config['host'], port=self.config['smtp_port']) as server:
                server.starttls()
                l = server.login(
                    self.config['smtp_user'], self.config['smtp_pw'])
                for uid, mail in mails:
                    subject = mail["Subject"]
                    if self._is_filtered(mail):
                        self.logger.info(
                            f'{uid}: `{subject}` has been filtered')
                    else:
                        del mail['To']
                        mail['To'] = ','.join(self._get_user_emails())
                        ret = server.sendmail(self.config['smtp_user'],
                                              self._get_user_emails(),
                                              mail.as_string())

                        if len(ret) > 0:
                            self.logger.error(
                                f'{uid}: `{subject}` sending failed')
                            for address, err in ret.items():
                                self.logger.error(f'{address}: {err}')
                        else:
                            self.logger.info(
                                f'{uid}: `{subject}` has been sent')

                        time.sleep(self.config['send_interval'])
        except Exception as e:
            self.logger.error(f'sending mail failed : {e}')
            self._send_mails_to_manager(self._create_error_mail(
                f'{dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} : Sending Mail Failed',
                str(e)))


def create_logger(logname, console=False):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    if console:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    if not os.path.exists(os.path.dirname(logname)):
        os.mkdir(os.path.dirname(logname))

    handler = TimedRotatingFileHandler(logname, when="midnight", interval=1)
    handler.suffix = "%Y%m%d"
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


def main():
    logger = create_logger('log/forward.log')
    logger.info('Start')

    forwarder = MailForwarder(
        config='config.json',
        last_send='last_send.txt',
        members='members.json',
        subject_filter='subject_filter.txt',
        logger=logger,
    )

    stop = False

    def signal_handler(sig, frame):
        nonlocal stop
        stop = True
        logger.info('Shutdown')

    def is_running():
        nonlocal stop
        return not stop

    signal.signal(signal.SIGINT, signal_handler)

    while is_running():
        forwarder.update()


if __name__ == '__main__':
    main()
