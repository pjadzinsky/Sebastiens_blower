# Based on https://github.com/returngreat/django-debug-email-backend/blob/master/debug_email_backend/backends.py

from django.conf import settings
from djrill.mail.backends.djrill import DjrillBackend

class Backend(DjrillBackend):
    def send_messages(self, email_messages):
        if getattr(settings, 'DEBUG_EMAIL', False):
            # We're in debug mode, so add recipients to the message,
            # then replace them with a redirection to the debug-email address
            for email_message in email_messages:
                body = email_message.body
                email_message.body = """
                Original Recipients
                  TO: %s
                  CC: %s
                  BCC: %s
                -------------
                %s""" % (email_message.to, email_message.cc, email_message.bcc,  body)
                email_message.to  = ["debug-email@mousera.com"]
                email_message.cc  = []
                email_message.bcc = []

        return super(Backend, self).send_messages(email_messages)
