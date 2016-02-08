
import time
imported = False
try:
    import requests
    imported = True
except Exception as e:
    print
    print "Couldn't import requests to post push notifications."
    print "Please 'pip install requests'"
    print
    time.sleep(2)
    pass

from common import settings


def send_room_message(room_names, message, from_user=None, color=None,
                      hipchat_token=settings.HIPCHAT_MOUSEBOT_TOKEN):
    '''

    :param room_names: room id or name, also takes a list of room names
    :param message: message to send
    :param from_user: User that sends the message
    :param color: one of "yellow", "red", "green", "purple", "gray", or "random", default = yellow
    :param hipchat_token:
    :return:
    '''

    if not imported:
        return False

    if isinstance(room_names, basestring):
        room_names = [room_names]

    for room_name in room_names:
        color = color or 'yellow'
        from_user = from_user or 'System'
        url = 'https://www.hipchat.com/v1/rooms/message'

        response = requests.post(url, params={'auth_token': hipchat_token,
                                              'message': message,
                                              'room_id': room_name,
                                              'color': color,
                                              'from': from_user,
                                              'message_format': 'text'})

        response.raise_for_status()
