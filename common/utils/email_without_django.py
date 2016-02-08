import mandrill
import base64

'''
ZBS 3 Mar 2016:
This is a direct interface to Mandrill email service that will be deprecated
once the event and notification system is complete.
'''

MANDRILL_DEVICE_API_KEY = 'YMudCQahxPWJhT1pXuCdKA'
    # This key is setup as in mandrill.com
    # under the "Settings" tab under "API Keys"
    # This line is the one called "devices"

def send_mail(to_emails, from_email, subject, message, attachments=None):
    if type(to_emails) is str:
        to_emails = [ to_emails ]
    client = mandrill.Mandrill(MANDRILL_DEVICE_API_KEY)
    message = {
        'to':[{'email':to} for to in to_emails],
        'text':message,
        'subject':subject,
        'from_email':from_email
    }
    if attachments is not None:
        attachment_list = []
        for attachment in attachments:
            try:
                with open( attachment ) as f:
                    content = base64.b64encode( f.read() )
                    attachment_list.append({
                        'content': content,
                        'name': attachment,
                        'type': 'text/plain'
                    })
            except Exception, e:
                # @TODO CONVERT to log
                print 'send_mail attachment exception', e
        message['attachments'] = attachment_list
    result = client.messages.send(async=False, message=message)
    return result
