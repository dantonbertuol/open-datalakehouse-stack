class Logs():
    '''
    Class to write logs
    '''
    def __init__(self, file: str) -> None:
        '''
        Constructor

        Args:
            file (str): file to write logs
        '''
        self.file = file

    def write(self, message: str) -> None:
        '''
        Write message in the log file

        Args:
            message (str): message to write
        '''
        with open(self.file, mode='a+', encoding='utf-8') as file:
            file.write(message + '\n')
