import ast
import json
from common.utils.mac_address import getSettingsMACAddress

class BaseMessage(object):
    # Base classes must set this.
    command_name = ''
    _message = {}

    def __new__(cls, *args, **kwargs):
        self = object.__new__(cls)
        self._message = args[0]
        return self

    def __init__(self, *args, **kwargs):
        self.validate()

    def validate(self):
        pass

    def validate_existence(self, key, value=None):
        """
        Validate that the message contains a "key".  Also, if provided,
        validate that the key equals "value".
        """
        if self._message.get(key) == None:
            raise Exception("Message did not validate: missing key '%s'" % key, self._message)
        if value and self._message[key] != value:
            raise Exception("Message did not validate: value '%s' should have been '%s' for key '%s'" %\
                  (self._message[key], value, key, ), self._message)

    def get(self, key, optional_value = None):
        return self._message.get(key, optional_value)

    def serialize(self):
        return json.dumps(self._message)

    @classmethod
    def deserialize(cls, LOGGER, message):
        try:
            if not type(message) is dict:
                message = json.loads(message)
            return cls(message)
        except Exception as e:
            LOGGER.warn("%s.deserialize: failed on message: %s (%s)" % (cls, message, e))
            return None

class CommandMessage(BaseMessage):
    """
    The base class for messages that pass commands around the system.
    """
    # Base classes must set this.
    command_name = ''

    def __init__(self, *args, **kwargs):
        # Verify that we have a command_name, then, if needed, set it.
        if not self.command_name:
            raise Exception("BaseMessage: error - did not set 'command' name in '%s'" %\
                            self.__class__.__name__)
        if not self._message.get('command'):
            self._message['command'] = self.command_name

        super(CommandMessage, self).__init__(*args, **kwargs)

    def validate(self):
        """
        Validate the message.
        """
        super(CommandMessage, self).validate()
        self.validate_existence('command', self.command_name)

class MacAddressCommandMessage(CommandMessage):
    """
    The base class for command messages that are filtered by MAC address.
    """
    def validate(self):
        """
        Validate the message.
        """
        super(CommandMessage, self).validate()
        self.validate_existence('mac_addresses')

    def check_mac_address(self):
        """
        This is not a 'validation'.  It's just a boolean check.
        """
        my_mac_address = unicode(getSettingsMACAddress())
        try:
            mac_addresses = self._message['mac_addresses']
            if not type(mac_addresses) == list:
                mac_addresses = ast.literal_eval(mac_addresses)
            mac_addresses = map(lambda m: m.replace(u"-",u":").lower(), mac_addresses)
            if my_mac_address in mac_addresses:
                return True
        except Exception as e:
            pass
        # No match.  Return false.
        return False

    @classmethod
    def deserialize(cls, *args, **kwargs):
        m = super(MacAddressCommandMessage, cls).deserialize(*args, **kwargs)
        # Are we a target of this message?
        if m and not m.check_mac_address():
            return None
        return m

