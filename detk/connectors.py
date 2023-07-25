import logging
import sys
from abc import ABC, abstractmethod

import pandas as pd
from sqlalchemy import create_engine

from detk.logger import logger


class DBConnector(ABC):
    """
    Base class for database connectors.
    """
    def __init__(self, **kwargs):
        self._host = kwargs.get('host')
        self._user = kwargs.get('user')
        self._pwd = kwargs.get('pwd')
        self._port = kwargs.get('port')
        self.db_name = kwargs.get('db_name')
        self.schema = kwargs.get('schema')
        self.driver = kwargs.get('driver')
        self.conn = None

    @abstractmethod
    def connect(self):
        raise NotImplementedError('A required method is missing implementation! Implement it to remove this error.')


class DBPDConnector(DBConnector):
    """
    Base class for connector which uses pandas read_sql function to read data from DB.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def _get_chunk_async(chunks):
        """Extracts next chunk from pandas dataframe iterator"""
        try:
            df = next(chunks)
            return df
        except StopIteration:
            return pd.DataFrame()

    async def execute_sql_chunks(self, sql_str, chunk_q, loop, chunksize):
        """Asynchronous execution of sql query in chunks.
        Each chunk is placed in queue to be processed further in the pipeline."""
        logger.info(f'Start: {sql_str}')

        idx = 0
        chunks = pd.read_sql(sql_str, self.conn, chunksize=chunksize)

        while True:
            logger.info(f'Extracting chunk {idx}: {sql_str}')
            df = await loop.run_in_executor(None, self._get_chunk_async, chunks)
            if df.empty:
                break
            logger.info(f'Chunk {idx} extracted')
            chunk_q.put((idx, df))
            idx += 1
        chunk_q.put(('DONE', None))

    async def execute_sql_df(self, sql_str, loop) -> pd.DataFrame:
        """Asynchronous running of sql extract into single dataframe."""
        logger.info(f'Starting extract: {sql_str}..')
        return await loop.run_in_executor(None, pd.read_sql, sql_str, self.conn)


class MSSqlPDConnector(DBPDConnector):
    """
    MSSql database connector which uses pandas methods to extract data
    """
    dialect = 'mssql+pyodbc'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self._port:
            self._port = '1433'
        if not self.driver:
            self.driver = 'ODBC+Driver+17+for+SQL+Server'
        self.connect()

    def connect(self):
        """Establishes connection with DB using SQL Alchemy"""
        url = f'{self.dialect}://{self._user}:{self._pwd}@{self._host}:{self._port}/{self.db_name}?driver={self.driver}'
        engine = create_engine(url)  #  MultipleActiveResultSets=True  #mars_connection=yes
        try:
            self.conn = engine.connect()
        except Exception as e:
            logging.error(f'Connection failed: {e}, exiting...')
            sys.exit(-1)
        logger.info('DB connected')















