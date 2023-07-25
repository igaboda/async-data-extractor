import os
import time
import sys
import argparse
from multiprocessing import cpu_count

import detk.dataprocessing
from detk.utils import load_dp_config, prepare_query_list
from detk.logger import setup_logger, logger


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-o', '--output', help='Output path for files', required=True)
    parser.add_argument('-l', '--logs', help='Output path for logs')
    parser.add_argument('-c', '--config', help='Path to alternative yaml pipeline config file')
    parser.add_argument('--chunks', help='Runs extraction in chunks, recommended for large datasets',
                        action='store_true')

    args = parser.parse_args()

    # data processing config
    dp_config = load_dp_config(args)
    if dp_config['process_in_chunks']:
        pipeline_cls = getattr(detk.dataprocessing, dp_config['pipeline']['name_chunks'])
    else:
        pipeline_cls = getattr(detk.dataprocessing, dp_config['pipeline']['name'])

    # logger
    setup_logger(os.path.join(dp_config['output_path'], 'logs.log'))

    if not dp_config['pipeline']['extract_tables'] and not dp_config['pipeline']['custom_extracts'][0]['table']:
        logger.error('Input values for data processing pipeline missing. '
                     'Parameters extract_tables or custom_extracts must be specified. ')
        sys.exit(-1)

    # queries
    query_list = prepare_query_list(dp_config)
    q_count = len(query_list)

    # multiple queries in chunks (multi processes)
    if dp_config['process_in_chunks'] and q_count > 1:
        p_count = cpu_count() - 1

        if q_count <= p_count:
            p_count = q_count

        for i in range(0, q_count, p_count):
            end = p_count + i
            if end > q_count:
                end = q_count

            processes = []
            for j in range(i, end):
                processes.append(pipeline_cls(query_list[j], dp_config))

            for p in processes:
                start_time = time.time()
                logger.info('START')
                p.start()

            for p in processes:
                p.join()
            logger.info(f'END. Extracting took: {round(time.time() - start_time, 1)}\n')

    # single query in chunks (single async process)
    elif dp_config['process_in_chunks'] and q_count == 1:
        pipeline_exec = pipeline_cls(query_list[0], dp_config)

        start_time = time.time()
        logger.info('START')
        pipeline_exec.run()
        logger.info(f'END. Extracting took: {round(time.time() - start_time, 1)}\n')

    # multiple queries, no chunks (single async process)
    else:
        pipeline_exec = pipeline_cls(query_list, dp_config)

        start_time = time.time()
        logger.info('START')
        pipeline_exec.run()
        logger.info(f'END. Extracting took: {round(time.time() - start_time, 1)}\n')







