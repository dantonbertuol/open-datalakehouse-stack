import requests


class Consumer():
    '''
    Consumer class to get data from an endpoint
    '''

    def __init__(self, endpoint: str) -> None:
        '''
        Constructor

        Args:
            endpoint (str): endpoint to get data
        '''
        self.endpoint: str = endpoint

    def get_data(self) -> dict:
        '''
        Get data from endpoint

        Returns:
            dict: data from endpoint
        '''
        response = requests.get(self.endpoint)
        return response.json()
