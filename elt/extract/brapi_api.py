from src.api.api_consumer import Consumer
from datetime import datetime
import sys
from pathlib import Path

DATA_LOG: str = datetime.now().strftime('%d-%m-%Y')
PROJECT_PATH = Path(__file__).absolute().parent.parent.parent
CHROME_DRIVER_PATH = f'{PROJECT_PATH}/bin/chromedriver/chromedriver'
LOG_PATH = f'{PROJECT_PATH}/logs/elt_moodle/log_' + DATA_LOG + '.txt'

sys.path.insert(1, str(PROJECT_PATH))


class BrapiAPI():
    '''
    Brapi API consumer
    '''
    def __init__(self, endpoint: str) -> None:
        '''
        Initialize BrapiAPI class

        Args:
            endpoint (str): API endpoint
        '''
        self.endpoint = endpoint
        self.consumer = Consumer(self.endpoint)

    def get_data(self, filter: str = "") -> dict:
        '''
        Get data from API

        Args:
            filter (str, optional): Filter get. Defaults to "".

        Returns:
            dict: Data from API
        '''
        data: dict = self.consumer.get_data(filter)
        return data


if __name__ == '__main__':
    endpoint: str = 'https://brapi.dev/api/available'
    brapi_api = BrapiAPI(endpoint)
    data: dict = brapi_api.get_data()
    print(data)
