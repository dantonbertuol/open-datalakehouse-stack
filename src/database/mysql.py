import pymysql
from pathlib import Path

PROJECT_PATH = Path(__file__).absolute().parent.parent.parent


class MySQL():
    '''
    Class to connect and manipulate MySQL database
    '''

    def __init__(self, host: str, user: str, password: str, database: str) -> None:
        '''
        Constructor

        Args:
            host (str): address of the database
            user (str): user to connect to the database
            password (str): password to connect to the database
            database (str): database name
        '''
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = pymysql.connect(host=self.host, user=self.user,
                                          password=self.password, database=self.database)
        self.cursor = self.connection.cursor()

    def verify_table_exists(self, table_name: str) -> bool:
        '''
        Verify if table exists in the database

        Args:
            table_name (str): table name

        Returns:
            bool: True if table exists, False otherwise
        '''
        sql = f'SHOW TABLES LIKE "{table_name}"'
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        if result:
            return True
        return False

    def create_table(self, path: str) -> list:
        '''
        Create a table in the database

        Args:
            table_name (str): table name
            columns (dict): columns of the table
        '''
        result: list = [True, '']

        try:
            fd = open(Path.joinpath(PROJECT_PATH, path), 'r')
            sql = fd.read()
            fd.close()
        except Exception as e:
            result = [False, e]

        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            result = [False, e]

        return result

    def insert_data(self, table: str, data: dict) -> list:
        '''
        Insert data in the database table

        Args:
            table (str): table name
            data (dict): data to insert
        '''
        result: list = [True, '']

        sql: str = f"INSERT INTO {table} ({','.join(list(data.keys()))}) VALUES ({'%s,' * len(list(data.keys()))}"
        sql = sql[:-1] + ')'

        try:
            self.cursor.execute(sql, list(data.values()))
            self.connection.commit()
        except pymysql.err.IntegrityError:  # Ignore duplicate data
            pass
        except Exception as e:  # Other errors
            self.connection.rollback()
            result = [False, e]

        return result

    def get_data(self, table: str, fields: str = "*", filter: str = "", order: str = "") -> tuple:
        '''
        Get data from database table

        Args:
            table (str): table name
            fields (str, optional): fields to get data. Defaults to "*".
            filter (str, optional): filter to apply in sql. Defaults to "".
            order (str, optional): order by. Defaults to "".

        Returns:
            tuple: tuple with data
        '''
        sql = f"SELECT {fields} FROM {table} "

        if filter != "":
            sql += f'WHERE {filter}'

        if order != "":
            sql += f'ORDER BY {order}'

        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            print('Erro ao buscar dados: ', e)
            return ('ERROR',)

    def truncate_table(self, table: str) -> list:
        '''
        Truncate table

        Args:
            table (str): table name

        Returns:
            list: list with result of truncate
        '''
        result: list = [True, '']

        sql = f'TRUNCATE TABLE {table}'

        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            result = [False, e]

        return result

    def close_connection(self) -> None:
        '''
        Close database connection
        '''
        self.connection.close()
