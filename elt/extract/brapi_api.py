from datetime import datetime
import sys
from pathlib import Path

DATA_LOG: str = datetime.now().strftime('%d-%m-%Y')
PROJECT_PATH = Path(__file__).absolute().parent.parent.parent
CHROME_DRIVER_PATH = f'{PROJECT_PATH}/bin/chromedriver/chromedriver'
LOG_PATH = f'{PROJECT_PATH}/logs/elt_moodle/log_' + DATA_LOG + '.txt'

sys.path.insert(1, str(PROJECT_PATH))  # insert path to run in windows
from src.api.api_consumer import Consumer  # noqa: E402


class BrapiAPI():
    '''
    Class to consume BrAPI API
    '''

    def __init__(self, endpoint: str) -> None:
        '''
        Constructor

        Args:
            endpoint (str): endpoint to consume
        '''
        self.endpoint = endpoint
        self.consumer = Consumer(self.endpoint)

    def get_data(self, filter: str = "") -> dict:
        '''
        Get data from endpoint

        Args:
            filter (str, optional): filter. Defaults to "".

        Returns:
            dict: data from endpoint
        '''
        data: dict = self.consumer.get_data(filter)
        return data


if __name__ == '__main__':
    endpoint: str = 'https://brapi.dev/api/available'
    brapi_api = BrapiAPI(endpoint)
    data: dict = brapi_api.get_data()
    print(data)
