import os
import csv
import asyncio
from abc import ABC
from queue import Empty

from detk.logger import logger


class DataWriter(ABC):
    """
    Base class for single query data writer.
    """
    def __init__(self, table, output_path):
        self.table = table
        self.output_path = output_path
        self.result_path = os.path.join(self.output_path, f'{self.table}.csv')


class PDCsvWriter(DataWriter):
    """
    Data writer which saves dataframes given in the queue to csv file using pandas method.
    """
    def __init__(self, table, output_path, chunk_q=None):
        super().__init__(table, output_path)
        self.chunk_q = chunk_q

    async def run_chunks(self):
        """Takes a dataframe from queue"""
        while True:
            try:
                idx, df_chunk = self.chunk_q.get(timeout=5)

                if str(idx) == 'DONE' and df_chunk is None:
                    logger.info('All chunks processed. Exiting PD CsvWriter')
                    break

                if idx == 0:
                    df_chunk.to_csv(self.result_path, index=False)
                else:
                    df_chunk.to_csv(self.result_path, index=False, header=False, mode='a')
                logger.info(f'Chunk {idx} saved to csv for {self.table}')

            except Empty:
                logger.info('Q empty. No chunks to save.')
                await asyncio.sleep(3)

    async def run_df(self, df):
        logger.info(f'saving {self.table} to csv..')
        df.to_csv(self.result_path, index=False)
