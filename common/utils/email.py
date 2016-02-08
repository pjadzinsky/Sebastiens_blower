from django.core import mail
from django.template.loader import get_template
from django.template import Context

'''
ZBS 3 Mar 2016: Note that this file requires django
To call email without django requirements, see email_without_django.py
'''

# Helper to template emails
def send_mail(from_email, to_emails, title, template, context, fail_silently = True, bcc=None):
    body = "\n[EOM]\n"
    if template:
        _template = get_template(template)
        body = _template.render(Context(context))

    if bcc:
        msg =  mail.EmailMultiAlternatives(title, body, from_email, to_emails, bcc=bcc)
        msg.send()
    else:
        return mail.send_mail(title, body, from_email, to_emails, fail_silently)

