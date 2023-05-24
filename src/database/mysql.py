import pymysql


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

    def create_table(self, table_name: str, columns: dict) -> None:
        '''
        Create a table in the database

        Args:
            table_name (str): table name
            columns (dict): columns of the table
        '''
        sql = f'CREATE TABLE {table_name} ('
        for column in columns:
            sql += f'{column} {columns[column]}, '
        sql = sql[:-2] + ')'

        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except Exception as e:
            print('Erro ao criar tabela: ', e)
            self.connection.rollback()

    def insert_data(self, table: str, data: dict) -> None:
        '''
        Insert data in the database table

        Args:
            table (str): table name
            data (dict): data to insert
        '''
        sql = f"INSERT INTO {table} ({','.join(list(data.keys()))}) VALUES ({'%s,' * len(list(data.keys()))}"
        sql = sql[:-1] + ')'

        try:
            self.cursor.execute(sql, list(data.values()))
            self.connection.commit()
        except Exception as e:
            print('Erro ao inserir dados: ', e)
            self.connection.rollback()

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
            sql += 'WHERE {filter}'

        if order != "":
            sql += 'ORDER BY {order}'

        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            print('Erro ao buscar dados: ', e)
            return ()

    def close_connection(self) -> None:
        '''
        Close database connection
        '''
        self.connection.close()
