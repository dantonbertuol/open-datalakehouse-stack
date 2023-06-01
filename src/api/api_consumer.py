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

    def get_data(self, filter: str = "") -> dict:
        '''
        Get data from endpoint

        Returns:
            dict: data from endpoint
        '''
        response = requests.get(self.endpoint + filter)
        try:
            json = response.json()
        except Exception as e:
            json = {'error': e}

        return json


if __name__ == '__main__':
    test = Consumer('https://brapi.dev/api/availabl/')
    test.get_data()
