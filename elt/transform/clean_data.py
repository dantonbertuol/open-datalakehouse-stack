from datetime import datetime
import sys
from pathlib import Path

DATA_LOG: str = datetime.now().strftime('%d-%m-%Y')
PROJECT_PATH = Path(__file__).absolute().parent.parent.parent
LOG_PATH = f'{PROJECT_PATH}/logs/stocks_pipeline/log_' + DATA_LOG + '.txt'

sys.path.insert(1, str(PROJECT_PATH))
from src.datalake.s3connect import S3Connect  # noqa: E402


class CleanData():
    '''
    Class to clean data
    '''
    def __init__(self, env: str) -> None:
        '''
        Initialize CleanData class

        Args:
            env (str): Environment
        '''
        self.s3 = S3Connect(env)

    def clean_table(self, path: str, bucket_from: str, bucket_to: str, fields: list):
        '''
        Function to clean table

        Args:
            path (str): path from file
            bucket_from (str): bucket from file
            bucket_to (str): bucket to write new file
            fields (list): fields to select
        '''
        try:
            df = self.s3.get_data(bucket_from, path, 'csv')

            clean_df = df.select(fields)

            self.s3.insert_data(clean_df, bucket_to, path, 'overwrite', 'parquet')
        except Exception as e:
            print(f"Error to clean table {path}: {e}")

    def close_s3_connection(self):
        '''
        Function to close S3 connection
        '''
        self.s3.close_spark_session()


if __name__ == '__main__':
    clean_data = CleanData('TESTE')

    fields = ['symbol']
    clean_data.clean_table('stocks/stock', 'landing', 'processing', fields)

    fields = ['symbol', 'longName', 'shortName', 'currency', 'marketCap',
              'regularMarketPrice', 'regularMarketVolume', 'regularMarketTime']
    clean_data.clean_table('stocks/stock_quotes', 'landing', 'processing', fields)

    clean_data.close_s3_connection()
