import asyncio
import multiprocessing
from abc import ABC, abstractmethod

import detk.connectors
import detk.writers
from detk.logger import logger


class Pipeline(ABC):
    """
    Base class for data processing pipelines.
    """
    def __init__(self, config):
        self.config = config

    @abstractmethod
    def process_pipeline(self):
        raise NotImplementedError('A required method is missing implementation! Implement it to remove this error.')

    @abstractmethod
    def run(self):
        raise NotImplementedError('A required method is missing implementation! Implement it to remove this error.')


class DataChunksPipeline(Pipeline, multiprocessing.Process):
    """
    Pipeline process for handling single query in chunks.
    Data chunk extracted by DB connector is placed in queue, from which chunk gets consumed by writer.
    """
    def __init__(self, table_query, config):
        Pipeline.__init__(self, config)
        multiprocessing.Process.__init__(self)
        self.table = table_query[1]
        self.sql_query = table_query[0]
        self.chunk_q = multiprocessing.Queue()
        logger.info('DataChunksPipeline initialized')

    async def process_pipeline(self):
        """Initializes workers and schedules tasks in the event loop"""
        # DB connector
        db_cls = getattr(detk.connectors, self.config['connector']['name'])
        db_connector = db_cls(**self.config['db_config'])

        # Writer Worker
        writer_cls = getattr(detk.writers, self.config['writer']['name'])
        data_writer = writer_cls(self.table, self.config['output_path'], self.chunk_q)

        # Event loop
        loop = asyncio.get_event_loop()

        await asyncio.gather(
            db_connector.execute_sql_chunks(self.sql_query, self.chunk_q, loop, self.config['pipeline']['chunk_size']),
            data_writer.run_chunks()
        )
        db_connector.conn.close()

    def run(self):
        """Start of the event loop"""
        asyncio.run(self.process_pipeline(), debug=True)


class DataPipeline(Pipeline):
    """
    Pipeline for processing list of queries asynchronously.
    """
    def __init__(self, query_list, config):
        super().__init__(config)
        self.query_list = query_list
        logger.info('Datapipeline initialized')

    async def process_pipeline(self):
        """Schedules process_query tasks in the event loop."""
        await asyncio.gather(*[self.process_query(*q) for q in self.query_list])

    async def process_query(self, sql_str, table):
        """Sets up data extraction process per single query."""
        # DB connector
        db_cls = getattr(detk.connectors, self.config['connector']['name'])
        db_connector = db_cls(**self.config['db_config'])

        # Writer Worker
        writer_cls = getattr(detk.writers, self.config['writer']['name'])
        data_writer = writer_cls(table, self.config['output_path'])

        # Event loop
        loop = asyncio.get_event_loop()

        # Process
        df = await db_connector.execute_sql_df(sql_str, loop)
        await data_writer.run_df(df)
        db_connector.conn.close()

    def run(self):
        """Start of the event loop"""
        asyncio.run(self.process_pipeline(), debug=True)


