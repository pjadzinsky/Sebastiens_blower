from .base import MacAddressCommandMessage

class ShutdownMessage(MacAddressCommandMessage):
    command_name = 'shutdown'

