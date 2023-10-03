from datetime import datetime
import sys
from pathlib import Path
from os import getenv

DATA_LOG: str = datetime.now().strftime('%d-%m-%Y')
PROJECT_PATH = Path(__file__).absolute().parent.parent
LOG_PATH = f'{PROJECT_PATH}/logs/stocks_pipeline/log_' + DATA_LOG + '.txt'

sys.path.insert(1, str(PROJECT_PATH))  # insert path to run in windows
from elt.extract.brapi_api import BrapiAPI  # noqa: E402
from elt.extract.yfinance_indicators import Indicators  # noqa: E402
from src.utils.logs import Logs  # noqa: E402
from src.database.mysql import MySQL  # noqa: E402


class ExtractData():
    '''
    Class to extract stocks data
    '''

    def __init__(self, env: str) -> None:
        '''
        Constructor method
        '''
        self.logs = Logs(LOG_PATH)
        self.env = env
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Starting Extract Data in {self.env}')

    def create_tables_struct(self) -> None:
        '''
        Create table stock structure
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Creating tables structure')

        database = MySQL(
            host='localhost',
            user='root',
            password='BrapiDev',
            database='stock_quotes'
        )

        result: list = [True, '']

        if not database.verify_table_exists('stock'):
            result = database.create_table('sql/create_tables/stock.sql')
            if result[0]:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Table stock created')
            else:
                self.logs.write(
                    f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Table stock not created. Error: {result[1]}')

        if not database.verify_table_exists('stock_quotes'):
            result = database.create_table('sql/create_tables/stock_quotes.sql')
            if result[0]:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Table stock_quotes created')
            else:
                self.logs.write(
                    f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Table stock_quotes not created.'
                    f' Error: {result[1]}')

        if not database.verify_table_exists('stock_indicators'):
            result = database.create_table('sql/create_tables/stock_indicators.sql')
            if result[0]:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Table stock_indicators created')
            else:
                self.logs.write(
                    f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Table stock_indicators not created.'
                    f' Error: {result[1]}')

        if not database.verify_table_exists('stock_dividends'):
            result = database.create_table('sql/create_tables/stock_dividends.sql')
            if result[0]:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Table stock_dividends created')
            else:
                self.logs.write(
                    f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Table stock_dividends not created.'
                    f' Error: {result[1]}')

        database.close_connection()

    def extract_api_data(self, endpoint: str, token: str) -> None:
        '''
        Extract data from api

        Args:
            endpoint (str): endpoint url

        Returns:
            dict: dict data
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Consuming endpoint {endpoint}')

        data: dict = {}
        result_: list = [True, '']
        database = MySQL(
            host='localhost',
            user='root',
            password='BrapiDev',
            database='stock_quotes'
        )

        # Available Endpoint
        if 'available' in endpoint:
            brapi_api = BrapiAPI(endpoint, token)
            data = brapi_api.get_data()
            if data.get('error') is None:
                stocks = data.get('stocks')

                for stock in stocks:  # type: ignore
                    # Não insere ações fracionarias
                    if stock.endswith('F'):
                        continue

                    result_ = database.insert_data('stock', {'symbol': stock})
                    if not result_[0]:
                        self.logs.write(
                            f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Stock {stock} not inserted.'
                            f' Error: {result_[1]}')
            else:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                                f'Error consuming endpoint {endpoint}')

        # Quote Endpoint
        elif 'quote' in endpoint:
            stocks = database.get_data('stock', 'symbol')
            stocks = [stock[0] for stock in stocks]

            # Somente busca cotação se retornou algum stock
            if len(stocks) > 0:
                quotes = BrapiAPI(endpoint, token)

                # Busca 200 por vez por limitação da API
                for stock in stocks:
                    quotes_data: dict = quotes.get_data(stock)

                    if quotes_data.get('error') is None:
                        for result in quotes_data.get('results'):  # type: ignore
                            if result.get('error'):
                                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                                                f'Error stock quote {result.get("symbol")} > {result.get("message")}')
                                result_ = [False, '']
                                continue

                            result_ = database.insert_data('stock_quotes', result)
                            if not result_[0]:
                                self.logs.write(
                                    f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                                    f'Stock Quote {result.get("symbol")} not inserted. Error: {result_[1]}')
                    else:
                        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                                        f'Error consuming endpoint {endpoint + stock} -> {quotes_data.get("error")}')
            else:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: No stock to search')

        database.close_connection()

    def extract_indicators(self):
        '''
        Extract indicators from yfinance api
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Extracting indicators')

        result: list = [True, '']
        indicators = Indicators()

        database = MySQL(
            host='localhost',
            user='root',
            password='BrapiDev',
            database='stock_quotes'
        )

        result = database.truncate_table('stock_indicators')

        if result[0]:
            stocks = database.get_data('stock', 'symbol')

            stocks = [stock[0] + '.SA' for stock in stocks]

            stock_indicators = indicators.get_indicators(' '.join(stocks))

            for stock in stock_indicators.keys():
                result = database.insert_data('stock_indicators', stock_indicators[stock])
                if not result[0]:
                    self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                                    f'Stock Indicator {stock} not inserted. Error: {result[1]}')
        else:
            self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                            f'Error truncating table stock_indicators. Error: {result[1]}')

        database.close_connection()

    def extract_dividends(self):
        '''
        Extract dividends from yfinance api
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Extracting dividends')

        indicators = Indicators()
        data: dict = {}
        result: list = [True, '']

        database = MySQL(
            host='localhost',
            user='root',
            password='BrapiDev',
            database='stock_quotes'
        )

        stocks = database.get_data('stock', 'symbol')

        stocks = [stock[0] + '.SA' for stock in stocks]

        stock_indicators = indicators.get_dividends(' '.join(stocks))

        for stock in stock_indicators.keys():
            for date, value in zip(stock_indicators[stock]['paymentDate'], stock_indicators[stock]['amount']):
                data = {
                    'symbol': stock,
                    'paymentDate': date,
                    'amount': value
                }
                result = database.insert_data('stock_dividends', data)
                if not result[0]:
                    self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                                    f'Stock Dividend {stock} not inserted. Error: {result[1]}')

        database.close_connection()

    def finish(self) -> None:
        '''
        Finish method
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Finish Extract Data in {self.env}')


if __name__ == '__main__':
    extract_data = ExtractData('TESTE')

    extract_data.create_tables_struct()

    token = getenv('BRAPI_TOKEN')  # Get token from environment variable
    extract_data.extract_api_data('https://brapi.dev/api/available/', token)
    extract_data.extract_api_data('https://brapi.dev/api/quote/', token)

    extract_data.extract_indicators()
    extract_data.extract_dividends()

    extract_data.finish()
