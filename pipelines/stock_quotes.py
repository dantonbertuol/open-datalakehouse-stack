from datetime import datetime
import sys
from pathlib import Path

DATA_LOG: str = datetime.now().strftime('%d-%m-%Y')
PROJECT_PATH = Path(__file__).absolute().parent.parent
LOG_PATH = f'{PROJECT_PATH}/logs/stocks_pipeline/log_' + DATA_LOG + '.txt'
BUCKET_DATALAKE = 'datalake'
BUCKET_LAKEHOUSE = 'lakehouse'

sys.path.insert(1, str(PROJECT_PATH))  # insert path to run in windows
from src.utils.logs import Logs  # noqa: E402
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
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Starting Stock Quotes Pipeline in {self.env}')

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
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Cleaning data')

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
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Converting to delta')

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
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Enriching table stock_quotes')

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

        enrich_delta.close_s3_connection()

    def enrich_indicators(self) -> None:
        '''
        Method to enrich delta table
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Enriching table stock_indicators')

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

        enrich_delta.close_s3_connection()

    def enrich_dividends(self) -> None:
        '''
        Method to enrich delta table
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Enriching table stock_dividends')

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

        enrich_delta.close_s3_connection()

    def gold_stock(self) -> None:
        '''
        Method to gold stock table
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Creating Gold table stock')

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

        gold_tables.close_s3_connection()

    def gold_stock_quotes(self) -> None:
        '''
        Method to gold stock quotes table
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Creating Gold table stock_quotes')

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

        gold_tables.close_s3_connection()

    def gold_stock_indicators(self) -> None:
        '''
        Method to gold stock indicators table
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Creating Gold table stock_indicators')

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

        gold_tables.close_s3_connection()

    def gold_stock_dividends(self) -> None:
        '''
        Method to gold stock dividends table
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Creating Gold table stock_dividends')

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

        gold_tables.close_s3_connection()

    def finish(self) -> None:
        '''
        Method to finish pipeline
        '''
        self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Stock Quotes Pipeline finished')


if __name__ == '__main__':
    pipeline = StockQuotesPipeline('TESTE')

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

    pipeline.finish()
