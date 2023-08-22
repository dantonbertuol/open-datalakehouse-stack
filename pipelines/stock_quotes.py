from datetime import datetime
import sys
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, FloatType

DATA_LOG: str = datetime.now().strftime('%d-%m-%Y')
PROJECT_PATH = Path(__file__).absolute().parent.parent
LOG_PATH = f'{PROJECT_PATH}/logs/stocks_pipeline/log_' + DATA_LOG + '.txt'

sys.path.insert(1, str(PROJECT_PATH))

from src.datalake.s3connect import S3Connect  # noqa: E402
from src.database.mysql import MySQL  # noqa: E402
from src.utils.logs import Logs  # noqa: E402
from elt.extract.brapi_api import BrapiAPI  # noqa: E402


class StockQuotesPipeline(Logs):
    '''
    All quotes pipeline source code
    '''

    def __init__(self) -> None:
        '''
        Initialize StockQuotesPipeline class
        '''
        self.logs = Logs(LOG_PATH)

    def create_table_stock_struct(self) -> None:
        '''
        Create table stock structure
        '''
        database = MySQL(
            host='localhost',
            user='root',
            password='BrapiDev',
            database='stock_quotes'
        )

        success: bool = False

        if not database.verify_table_exists('stock'):
            success = database.create_table(
                'stock',
                {
                    'id': 'INT AUTO_INCREMENT',
                    'symbol': 'VARCHAR(10) NOT NULL',
                    'primary key': '(id)',
                    'unique': '(symbol)'
                }
            )
            if success:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Table stock created')
            else:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Table stock not created')

        database.close_connection()

    def create_table_stock_quotes_struct(self) -> None:
        '''
        Create table stock quotes structure
        '''
        database = MySQL(
            host='localhost',
            user='root',
            password='BrapiDev',
            database='stock_quotes'
        )

        success: bool = False

        if not database.verify_table_exists('stock_quotes'):
            success = database.create_table(
                'stock_quotes',
                {
                    "id": "INT AUTO_INCREMENT",
                    "symbol": "VARCHAR(10) NOT NULL",
                    "shortName": "VARCHAR(100)",
                    "longName": "VARCHAR(100)",
                    "currency": "VARCHAR(10)",
                    "regularMarketPrice": "FLOAT",
                    "regularMarketDayHigh": "FLOAT",
                    "regularMarketDayLow": "FLOAT",
                    "regularMarketDayRange": "VARCHAR(50)",
                    "regularMarketChange": "FLOAT",
                    "regularMarketChangePercent": "FLOAT",
                    "regularMarketTime": "TIMESTAMP",
                    "marketCap": "FLOAT",
                    "regularMarketVolume": "FLOAT",
                    "regularMarketPreviousClose": "FLOAT",
                    "regularMarketOpen": "FLOAT",
                    "averageDailyVolume10Day": "FLOAT",
                    "averageDailyVolume3Month": "FLOAT",
                    "fiftyTwoWeekLowChange": "FLOAT",
                    "fiftyTwoWeekLowChangePercent": "FLOAT",
                    "fiftyTwoWeekRange": "VARCHAR(50)",
                    "fiftyTwoWeekHighChange": "FLOAT",
                    "fiftyTwoWeekHighChangePercent": "FLOAT",
                    "fiftyTwoWeekLow": "FLOAT",
                    "fiftyTwoWeekHigh": "FLOAT",
                    "twoHundredDayAverage": "FLOAT",
                    "twoHundredDayAverageChange": "FLOAT",
                    "twoHundredDayAverageChangePercent": "FLOAT",
                    'primary key': '(id)',
                    'unique': '(symbol,regularMarketTime)'
                }
            )
            if success:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Table stock_quotes created')
            else:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: Table stock_quotes not created')

    def extract_api_data(self, endpoint: str) -> None:
        '''
        Extract data from api

        Args:
            endpoint (str): endpoint url

        Returns:
            dict: dict data
        '''
        data: dict = {}

        s3 = S3Connect('TESTE')

        first: bool = True

        # Available Endpoint
        if 'available' in endpoint:
            brapi_api = BrapiAPI(endpoint)
            data = brapi_api.get_data()
            if data.get('error') is None:
                stocks = data.get('stocks')
                s3.insert_data(stocks, 'landing', 'stocks/available_stocks', 'overwrite')
            else:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                                f'Error consuming endpoint {endpoint}')

        # Quote Endpoint
        elif 'quote' in endpoint:
            stocks = s3.get_data('landing', 'stocks/available_stocks', 'csv').collect()
            stocks = [stock.value for stock in stocks]

            quote_schema = self.get_schema('quotes')

            # Somente busca cotação se retornou algum stock
            if len(stocks) > 0:
                quotes = BrapiAPI(endpoint)

                quotes_data: dict = quotes.get_data(",".join(stocks))
                if quotes_data.get('error') is None:
                    result = quotes_data.get('results')
                    for stock in result:  # type: ignore
                        for key, value in stock.items():
                            if type(value) == int:
                                try:
                                    stock[key] = float(value)
                                except Exception:
                                    pass
                    if first:
                        s3.insert_data(quotes_data.get('results'), 'landing',
                                       'stocks/stock_quotes', 'overwrite', quote_schema)
                        first = False
                    else:
                        s3.insert_data(quotes_data.get('results'), 'landing',
                                       'stocks/stock_quotes', 'append', quote_schema)
                else:
                    self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: '
                                    f'Error consuming endpoint {endpoint}')
            else:
                self.logs.write(f'{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: No stock to search')

    def get_schema(self, table: str) -> StructType:
        '''
        Get schema from table

        Args:
            table (str): table name

        Returns:
            StructType: schema from table
        '''
        struct: StructType = StructType([])

        if table == 'quotes':
            struct = StructType([
                StructField("symbol", StringType(), True),
                StructField("shortName", StringType(), True),
                StructField("longName", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("regularMarketPrice", FloatType(), True),
                StructField("regularMarketDayHigh", FloatType(), True),
                StructField("regularMarketDayLow", FloatType(), True),
                StructField("regularMarketDayRange", StringType(), True),
                StructField("regularMarketChange", FloatType(), True),
                StructField("regularMarketChangePercent", FloatType(), True),
                StructField("regularMarketTime", StringType(), True),
                StructField("marketCap", FloatType(), True),
                StructField("regularMarketVolume", FloatType(), True),
                StructField("regularMarketPreviousClose", FloatType(), True),
                StructField("regularMarketOpen", FloatType(), True),
                StructField("averageDailyVolume10Day", FloatType(), True),
                StructField("averageDailyVolume3Month", FloatType(), True),
                StructField("fiftyTwoWeekLowChange", FloatType(), True),
                StructField("fiftyTwoWeekLowChangePercent", FloatType(), True),
                StructField("fiftyTwoWeekRange", StringType(), True),
                StructField("fiftyTwoWeekHighChange", FloatType(), True),
                StructField("fiftyTwoWeekHighChangePercent", FloatType(), True),
                StructField("fiftyTwoWeekLow", FloatType(), True),
                StructField("fiftyTwoWeekHigh", FloatType(), True),
                StructField("twoHundredDayAverage", FloatType(), True),
                StructField("twoHundredDayAverageChange", FloatType(), True),
                StructField("twoHundredDayAverageChangePercent", FloatType(), True)
            ])

        return struct


if __name__ == '__main__':
    pipeline = StockQuotesPipeline()
    # pipeline.create_table_stock_struct()
    # pipeline.create_table_stock_quotes_struct()
    pipeline.extract_api_data('https://brapi.dev/api/available/')
    pipeline.extract_api_data('https://brapi.dev/api/quote/')
