from .base import MacAddressCommandMessage

class ReloadDeviceManagerMessage(MacAddressCommandMessage):
    command_name = 'reload_device_manager'
