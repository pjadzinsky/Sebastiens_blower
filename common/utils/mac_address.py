import fcntl, socket, struct

# TODO: this code needs to be refactored so that it works
# both for mouseapp and for device_manager
def getSettingsMACAddress():
    """
    Helper which assumes we have a 'settings' module
    """

    # Cache the result
    from device_manager import settings
    if not hasattr(getSettingsMACAddress, '_address'):
        getSettingsMACAddress._address =\
            getMACAddress(getattr(settings, 'NETWORK_INTERFACE'))

    return getSettingsMACAddress._address

def getMACAddress(interface_name):
    """
    Borrowed from http://stackoverflow.com/questions/159137/getting-mac-address
    Usage: getMACAddress('wlan0')
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    info = fcntl.ioctl(s.fileno(), 0x8927,  struct.pack('256s', interface_name[:15]))
    return ''.join(['%02x:' % ord(char) for char in info[18:24]])[:-1]

def clean(m):
    return str(m).replace(":", "").replace("-","").lower()
  
def clean_and_reverse(m):
    return clean(m)[::-1]


