import json
from pathlib import Path

PROJECT_PATH = Path(__file__).absolute().parent.parent.parent


class Util():
    '''
    Util class
    '''
    @staticmethod
    def get_credential(service: str) -> dict:
        '''
        Get credential from json file

        Args:
            service (str): service to get credential

        Returns:
            dict: credential
        '''
        with open(f'{PROJECT_PATH}/config/credential.json') as json_file:
            credential = json.load(json_file)
            return credential[service]
