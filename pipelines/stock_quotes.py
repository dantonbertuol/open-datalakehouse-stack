from src.database.mysql import MySQL
from datetime import datetime
# import sys
from pathlib import Path

DATA_LOG: str = datetime.now().strftime('%d-%m-%Y')
PROJECT_PATH = Path(__file__).absolute().parent.parent
CHROME_DRIVER_PATH = f'{PROJECT_PATH}/bin/chromedriver/chromedriver'
LOG_PATH = f'{PROJECT_PATH}/logs/elt_moodle/log_' + DATA_LOG + '.txt'

# sys.path.insert(1, str(PROJECT_PATH))


class StockQuotesPipeline():
    '''
    All quotes pipeline source code
    '''

    def create_table_stock_struct(self):
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

    def create_table_stock_quotes_struct(self):
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
                    'primary key': '(id)'
                }
            )


if __name__ == '__main__':
    pipeline = StockQuotesPipeline()
    pipeline.create_table_stock_struct()
    pipeline.create_table_stock_quotes_struct()
