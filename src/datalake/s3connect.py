from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.types import StringType
import sys

PROJECT_PATH = Path(__file__).absolute().parent.parent.parent
sys.path.insert(1, str(PROJECT_PATH))
from src.utils.util import Util  # noqa: E402


class S3Connect():
    '''
    Class to connect in S3
    '''

    def __init__(self, env: str):
        '''
        Initialize S3Connect class

        Args:
            env (str): Environment
        '''
        credential = Util.get_credential('MINIO')
        self.user = credential[env]['USER']
        self.psw = credential[env]['PSW']
        self.endpoint = credential[env]['ENDPOINT']
        self.spark = self.get_spark_session()
        self.spark.sparkContext.setLogLevel("ERROR")

    def get_spark_session(self) -> SparkSession:
        '''
        Get spark session

        Returns:
            SparkSession: Spark session
        '''
        builder = SparkSession \
            .builder \
            .appName("connect-s3") \
            .config("fs.s3a.endpoint", self.endpoint) \
            .config("fs.s3a.access.key", self.user) \
            .config("fs.s3a.secret.key", self.psw) \
            .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.hadoop.fs.s3a.fast.upload", True) \
            .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
            .config("fs.s3a.connection.maximum", 100) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
            .config("spark.sql.session.timeZone", "America/Sao_Paulo")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        return spark

    def insert_data(self, df, bucket_name: str, path: str, mode: str, schema=None):
        '''
        Insert data in S3

        Args:
            df: dataframe to insert
            bucket_name (str): bucket name
            path (str): path to insert
            mode (str): mode to insert
            schema (optional): data schema. Defaults to None.
        '''
        if type(df) == list and path == 'stocks/available_stocks':
            df = self.spark.createDataFrame(df, StringType())
        elif type(df) == list and path == 'stocks/stock_quotes':
            df = self.spark.createDataFrame(df, schema)

        df.write.option("header", "true").mode(mode).option(
            "compression", "gzip").csv(f"s3a://{bucket_name}/{path}")

    def get_data(self, bucket_name: str, path: str, type: str):
        '''
        Get data from S3

        Args:
            bucket_name (str): bucket name
            path (str): path to get
            type (str): type to get

        Returns:
            dataframe: dataframe from S3
        '''
        if type == 'csv':
            return self.spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
                f"s3a://{bucket_name}/{path}")
