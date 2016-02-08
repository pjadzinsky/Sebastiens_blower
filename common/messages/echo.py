from .base import CommandMessage

class EchoMessage(CommandMessage):
    command_name = 'echo'

    def validate(self):
        # This might throw an exception, but we'll pass it along
        super(EchoMessage, self).validate()
        self.validate_existence('text')