from datetime import datetime
import sys
from pathlib import Path

DATA_LOG: str = datetime.now().strftime('%d-%m-%Y')
PROJECT_PATH = Path(__file__).absolute().parent.parent.parent
LOG_PATH = f'{PROJECT_PATH}/logs/stocks_pipeline/log_' + DATA_LOG + '.txt'

sys.path.insert(1, str(PROJECT_PATH))
from src.datalake.s3connect import S3Connect  # noqa: E402


class EnrichDelta():
    '''
    Class to enrich data
    '''

    def __init__(self, env: str) -> None:
        '''
        Initialize EnrichDelta class

        Args:
            env (str): Environment
        '''
        self.s3 = S3Connect(env)

    def enrich_table(self, path_from: list, bucket_from: list, table_from: list, bucket_to: str,
                     path_to: str, table_to: str):
        '''
        Function to enrich table

        Args:
            path (str): path from file
            bucket_from (str): bucket from file
            bucket_to (str): bucket to write new file
            fields (list): fields to select
        '''
        try:
            for rpath, rbucket_from, rtable in zip(path_from, bucket_from, table_from):
                df = self.s3.get_data(rbucket_from, rpath, 'delta')

                df.createOrReplaceTempView(rtable)

            fd = open(Path.joinpath(PROJECT_PATH, f"sql/enrich_delta/{table_to}.sql"), 'r')
            sqlFile = fd.read()
            fd.close()

            df_final = self.s3.get_data(file_type='sql', sql=sqlFile)

            self.s3.insert_data(df_final, bucket_to, path_to, 'overwrite', 'delta', None, table_to)
        except Exception as e:
            print(f"Error to enrich table {table_to}: {e}")

    def close_s3_connection(self):
        '''
        Function to close S3 connection
        '''
        self.s3.close_spark_session()
