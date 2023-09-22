from datetime import datetime
import sys
from pathlib import Path

DATA_LOG: str = datetime.now().strftime('%d-%m-%Y')
PROJECT_PATH = Path(__file__).absolute().parent.parent
LOG_PATH = f'{PROJECT_PATH}/logs/stocks_pipeline/log_' + DATA_LOG + '.txt'
BUCKET_DATALAKE = 'datalake'
BUCKET_LAKEHOUSE = 'lakehouse'

sys.path.insert(1, str(PROJECT_PATH))  # insert path to run in windows
from elt.extract.brapi_api import BrapiAPI  # noqa: E402
from elt.extract.yfinance_indicators import Indicators  # noqa: E402
from src.utils.logs import Logs  # noqa: E402
from src.database.mysql import MySQL  # noqa: E402
from elt.transform.clean_data import CleanData  # noqa: E402
from elt.transform.convert_to_delta import ConvertDeltaTables  # noqa: E402
from elt.transform.enrich_delta import EnrichDelta  # noqa: E402
from elt.transform.gold_tables import GoldTables  # noqa: E402


class StockQuotesPipeline(Logs):
    '''
    All quotes pipeline source code
    '''

    def __init__(self, env: str) -> None:
        '''
        Constructor method
        '''
        self.logs = Logs(LOG_PATH)
        self.env = env

    def create_tables_struct(self) -> None:
        '''
        Create table stock structure
        '''
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

    def extract_api_data(self, endpoint: str) -> None:
        '''
        Extract data from api

        Args:
            endpoint (str): endpoint url

        Returns:
            dict: dict data
        '''
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
            brapi_api = BrapiAPI(endpoint)
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
                quotes = BrapiAPI(endpoint)

                # Busca 200 por vez por limitação da API
                for i in range(0, len(stocks), 200):
                    quotes_data: dict = quotes.get_data(",".join(stocks[i:i+200]))

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
                                        f'Error consuming endpoint {endpoint} -> {quotes_data.get("error")}')
            else:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: No stock to search')

        database.close_connection()

    def extract_indicators(self):
        '''
        Extract indicators from yfinance api
        '''
        indicators = Indicators()

        database = MySQL(
            host='localhost',
            user='root',
            password='BrapiDev',
            database='stock_quotes'
        )

        stocks = database.get_data('stock', 'symbol')

        stocks = [stock[0] + '.SA' for stock in stocks]

        stock_indicators = indicators.get_indicators(' '.join(stocks))

        for stock in stock_indicators.keys():
            result = database.insert_data('stock_indicators', stock_indicators[stock])
            if not result[0]:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                                f'Stock Indicator {stock} not inserted. Error: {result[1]}')

    def extract_dividends(self):
        '''
        Extract dividends from yfinance api
        '''
        indicators = Indicators()
        data: dict = {}

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

    def clean_data(self, path: list, bucket_from: list, bucket_to: list, fields: list) -> None:
        '''
        Method to clean data

        Args:
            env (str): environment
            path (list): list of paths to clean
            bucket_from (list): list of buckets from get data to clean
            bucket_to (list): list of buckets to write data cleaned
            fields (list): list of fields to select
        '''
        result: list = [True, '']

        clean_data = CleanData(self.env)

        for rpath, rbucket_from, rbucket_to, rfields in zip(path, bucket_from, bucket_to, fields):
            result = clean_data.clean_table(rpath, rbucket_from, rbucket_to, rfields)
            if not result[0]:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                                f'Error cleaning table {rpath}. Error: {result[1]}')

        clean_data.close_s3_connection()

    def convert_to_delta(self) -> None:
        '''
        Method to convert to delta tables

        Args:
            env (str): environment
            paths (list): list of paths to convert
            bucket_from (str): bucket from get data to convert
            bucket_to (str): bucket to write data converted
        '''
        result: list = [True, '']

        convert_delta = ConvertDeltaTables(self.env)

        paths = ['stocks/stock', 'stocks/stock_quotes', 'stocks/stock_indicators', 'stocks/stock_dividends']

        for path in paths:
            result = convert_delta.convert_table(path, BUCKET_DATALAKE, BUCKET_LAKEHOUSE)
            if not result[0]:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                                f'Error converting table {path}. Error: {result[1]}')

        convert_delta.close_s3_connection()

    def enrich_quote(self) -> None:
        '''
        Method to enrich delta table
        '''
        result: list = [True, '']

        enrich_delta = EnrichDelta(self.env)

        views: dict = {
            'stock': 'bronze/stocks/stock',
            'stock_quotes': 'bronze/stocks/stock_quotes'
        }

        path_to = 'silver/stocks/'
        table_to = 'stocks_quotes'

        result = enrich_delta.enrich_table(views, BUCKET_LAKEHOUSE, path_to, table_to)

        if not result[0]:
            self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                            f'Error enriching table {table_to}. Error: {result[1]}')

    def enrich_indicators(self) -> None:
        '''
        Method to enrich delta table
        '''
        result: list = [True, '']

        enrich_delta = EnrichDelta(self.env)

        views: dict = {
            'stock_indicators': 'bronze/stocks/stock_indicators',
        }

        path_to = 'silver/stocks/'
        table_to = 'stock_indicators'

        result = enrich_delta.enrich_table(views, BUCKET_LAKEHOUSE, path_to, table_to)

        if not result[0]:
            self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                            f'Error enriching table {table_to}. Error: {result[1]}')

    def enrich_dividends(self) -> None:
        '''
        Method to enrich delta table
        '''
        result: list = [True, '']

        enrich_delta = EnrichDelta(self.env)

        views: dict = {
            'stock_dividends': 'bronze/stocks/stock_dividends',
        }

        path_to = 'silver/stocks/'
        table_to = 'stock_dividends'

        result = enrich_delta.enrich_table(views, BUCKET_LAKEHOUSE, path_to, table_to)

        if not result[0]:
            self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                            f'Error enriching table {table_to}. Error: {result[1]}')

    def gold_stock(self) -> None:
        '''
        Method to gold stock table
        '''
        result: list = [True, '']

        gold_tables = GoldTables(self.env)

        views: dict = {
            'stock': 'bronze/stocks/stock',
            'stock_quotes': 'bronze/stocks/stock_quotes'
        }

        path_to = 'gold/stocks/'
        table_to = 'stock'

        result = gold_tables.gold_table(views, BUCKET_LAKEHOUSE, path_to, table_to)

        if not result[0]:
            self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                            f'Error golding table {table_to}. Error: {result[1]}')

    def gold_stock_quotes(self) -> None:
        '''
        Method to gold stock quotes table
        '''
        result: list = [True, '']

        gold_tables = GoldTables(self.env)

        views: dict = {
            'stock_quotes': 'bronze/stocks/stock_quotes',
        }

        path_to = 'gold/stocks/'
        table_to = 'stock_quotes'

        result = gold_tables.gold_table(views, BUCKET_LAKEHOUSE, path_to, table_to)

        if not result[0]:
            self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                            f'Error golding table {table_to}. Error: {result[1]}')

    def gold_stock_indicators(self) -> None:
        '''
        Method to gold stock indicators table
        '''
        result: list = [True, '']

        gold_tables = GoldTables(self.env)

        views: dict = {
            'stock_indicators': 'silver/stocks/stock_indicators',
        }

        path_to = 'gold/stocks/'
        table_to = 'stock_indicators'

        result = gold_tables.gold_table(views, BUCKET_LAKEHOUSE, path_to, table_to)

        if not result[0]:
            self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                            f'Error golding table {table_to}. Error: {result[1]}')

    def gold_stock_dividends(self) -> None:
        '''
        Method to gold stock dividends table
        '''
        result: list = [True, '']

        gold_tables = GoldTables(self.env)

        views: dict = {
            'stock_dividends': 'silver/stocks/stock_dividends',
        }

        path_to = 'gold/stocks/'
        table_to = 'stock_dividends'

        result = gold_tables.gold_table(views, BUCKET_LAKEHOUSE, path_to, table_to)

        if not result[0]:
            self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                            f'Error golding table {table_to}. Error: {result[1]}')


if __name__ == '__main__':
    pipeline = StockQuotesPipeline('TESTE')

    pipeline.create_tables_struct()
    pipeline.extract_api_data('https://brapi.dev/api/available/')
    pipeline.extract_api_data('https://brapi.dev/api/quote/')

    pipeline.extract_indicators()
    pipeline.extract_dividends()

    # tables_to_clean = ['stocks/stock', 'stocks/stock_quotes']
    # buckets_from = ['landing', 'landing']
    # buckets_to = ['processing', 'processing']
    # fields = [['symbol'], ['symbol', 'longName', 'shortName', 'currency', 'marketCap',
    #           'regularMarketPrice', 'regularMarketVolume', 'regularMarketTime']]
    # pipeline.clean_data('TESTE', tables_to_clean, buckets_from, buckets_to, fields)

    pipeline.convert_to_delta()

    pipeline.enrich_quote()
    pipeline.enrich_indicators()
    pipeline.enrich_dividends()

    pipeline.gold_stock()
    pipeline.gold_stock_quotes()
    pipeline.gold_stock_indicators()
    pipeline.gold_stock_dividends()
