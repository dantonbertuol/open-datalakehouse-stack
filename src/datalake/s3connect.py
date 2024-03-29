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
            .config("spark.sql.session.timeZone", "America/Sao_Paulo") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        return spark

    def insert_data(self, df, bucket_name: str, path: str, mode: str, file_type: str, schema=None, table: str = None):
        '''
        Insert data in S3

        Args:
            df: dataframe to insert
            bucket_name (str): bucket name
            path (str): path to insert
            mode (str): mode to insert
            fomart (str): type to insert
            schema (optional): data schema. Defaults to None.
            table (str, optional): table name. Defaults to None.
        '''
        if file_type == 'csv':
            if type(df) == list and path == 'stocks/available_stocks':
                df = self.spark.createDataFrame(df, StringType())
            elif type(df) == list and path == 'stocks/stock_quotes':
                df = self.spark.createDataFrame(df, schema)

            df.write.option("header", "true").mode(mode).option(
                "compression", "gzip").csv(f"s3a://{bucket_name}/{path}")
        elif file_type == 'parquet':
            df.write.format("parquet").mode("overwrite").save(
                f"s3a://{bucket_name}/{path}.parquet")
        elif file_type == 'delta':
            df.write.format("delta").mode("overwrite"). \
                option("mergeSchema", "true").option("path", f"s3a://{bucket_name}/{path}").save()
        elif file_type == 'deltatable':
            df.write.format("delta").mode("overwrite"). \
                option("mergeSchema", "true").option("path", f"s3a://{bucket_name}/{path}").saveAsTable(table)

    def get_data(self, bucket_name: str = '', path: str = '', file_type: str = '', sql: str = ''):
        '''
        Get data from S3

        Args:
            bucket_name (str, optional): bucket name. Defaults to ''.
            path (str, optional): path. Defaults to ''.
            file_type (str, optional): file type. Defaults to ''.
            sql (str, optional): sql. Defaults to ''.

        Returns:
            dataframe: dataframe from S3
        '''
        if file_type == 'csv':
            return self.spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
                f"s3a://{bucket_name}/{path}")
        elif file_type == 'parquet':
            return self.spark.read.parquet(f"s3a://{bucket_name}/{path}")
        elif file_type == 'delta':
            return self.spark.read.format("delta").load(f"s3a://{bucket_name}/{path}")
        elif file_type == 'sql':
            return self.spark.sql(sql)

    def load_temp_delta_view(self, bucket: str, views: dict) -> None:
        '''
        Load temp delta view

        Args:
            views (dict): dict of view names and paths
        '''
        for view, path in views.items():
            self.get_data(bucket, path, 'delta').createOrReplaceTempView(view)

    def drop_temp_delta_view(self, views: dict) -> None:
        '''
        Drop temp delta view

        Args:
            views (dict): dict of view names and paths
        '''
        for view in views.keys():
            self.spark.catalog.dropTempView(view)

    def close_spark_session(self):
        '''
        Close spark session
        '''
        self.spark.stop()
