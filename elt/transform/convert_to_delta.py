from datetime import datetime
import sys
from pathlib import Path

DATA_LOG: str = datetime.now().strftime('%d-%m-%Y')
PROJECT_PATH = Path(__file__).absolute().parent.parent.parent
LOG_PATH = f'{PROJECT_PATH}/logs/stocks_pipeline/log_' + DATA_LOG + '.txt'

sys.path.insert(1, str(PROJECT_PATH))
from src.datalake.s3connect import S3Connect  # noqa: E402


class ConvertDeltaTables():
    def __init__(self, env: str) -> None:
        '''
        Initialize ConvertDeltaTables class

        Args:
            env (str): Environment
        '''
        self.s3 = S3Connect(env)

    def convert_table(self, path: str, bucket_from: str, bucket_to: str):
        '''
        Function to convert file to delta table

        Args:
            path (str): path from file
            bucket_from (str): bucket from file
            bucket_to (str): bucket to write new file
        '''
        df = self.s3.get_data(bucket_from, f'{path}.parquet', 'parquet')

        df.write.mode("overwrite").format("delta").option(
            "mergeSchema", "true").save(f"s3a://{bucket_to}/bronze/{path}/")

    def close_s3_connection(self):
        '''
        Function to close S3 connection
        '''
        self.s3.close_spark_session()
