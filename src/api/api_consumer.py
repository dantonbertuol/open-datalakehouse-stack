import requests
from os import getenv


class Consumer():
    '''
    Consumer class to get data from an endpoint
    '''

    def __init__(self, endpoint: str, token: str = "") -> None:
        '''
        Constructor

        Args:
            endpoint (str): endpoint to get data
        '''
        self.endpoint: str = endpoint
        self.token: str = token

    def get_data(self, filter: str = "") -> dict:
        '''
        Get data from endpoint

        Returns:
            dict: data from endpoint
        '''
        url: str = self.endpoint + filter

        if self.token != "":
            url += f'?token={self.token}'

        try:
            response = requests.get(url)
            json = response.json()
        except Exception as e:
            json = {'error': e}

        return json


if __name__ == '__main__':
    token = getenv('BRAPI_TOKEN')  # Get token from environment variable
    test = Consumer('https://brapi.dev/api/quote/', token)
    test.get_data('AAPL34')
