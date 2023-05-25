from src.database.mysql import MySQL
from elt.extract.brapi_api import BrapiAPI
from datetime import datetime
# import sys
from pathlib import Path

DATA_LOG: str = datetime.now().strftime('%d-%m-%Y')
PROJECT_PATH = Path(__file__).absolute().parent.parent
LOG_PATH = f'{PROJECT_PATH}/logs/stocks_pipeline/log_' + DATA_LOG + '.txt'

# sys.path.insert(1, str(PROJECT_PATH))


class StockQuotesPipeline():
    '''
    All quotes pipeline source code
    '''

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

        if not database.verify_table_exists('stock'):
            database.create_table(
                'stock',
                {
                    'id': 'INT AUTO_INCREMENT',
                    'symbol': 'VARCHAR(10) NOT NULL',
                    'primary key': '(id)',
                    'unique': '(symbol)'
                }
            )

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

        if not database.verify_table_exists('stock_quotes'):
            database.create_table(
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

    def extract_api_data(self, endpoint: str) -> None:
        '''
        Extract data from api

        Args:
            endpoint (str): endpoint url

        Returns:
            dict: dict data
        '''
        data: dict = {}
        database = MySQL(
            host='localhost',
            user='root',
            password='BrapiDev',
            database='stock_quotes'
        )

        if 'available' in endpoint:
            brapi_api = BrapiAPI(endpoint)
            data = brapi_api.get_data()
            stocks = data.get('stocks')

            for stock in stocks:
                database.insert_data('stock', {'symbol': stock})

        elif 'quote' in endpoint:
            stocks = database.get_data('stock', 'symbol')
            stocks = [stock[0] for stock in stocks]

            # Somente busca cotação se retornou algum stock
            if len(stocks) > 0:
                quotes = BrapiAPI(endpoint)

                # Busca 200 por vez por limitação da API
                for i in range(0, len(stocks), 200):
                    quotes_data: dict = quotes.get_data(",".join(stocks[i:i+200]))
                    for result in quotes_data.get('results'):
                        if result.get('regularMarketTime') is not None:
                            result['regularMarketTime'] = datetime.strptime(
                                result.get('regularMarketTime'), '%Y-%m-%dT%H:%M:%S.%fZ')
                        database.insert_data('stock_quotes', result)

        database.close_connection()


if __name__ == '__main__':
    pipeline = StockQuotesPipeline()
    pipeline.create_table_stock_struct()
    pipeline.create_table_stock_quotes_struct()
    pipeline.extract_api_data('https://brapi.dev/api/available/')
    pipeline.extract_api_data('https://brapi.dev/api/quote/')
