import os
from typing import Dict

import yaml
from dotenv import load_dotenv


def load_db_config(env_path: [str, None] = None):
    """Reads Database configuration settings from environment variables."""
    load_dotenv(dotenv_path=env_path)

    db_config = {}
    db_config['host'] = os.environ.get('DB_HOST')
    db_config['user'] = os.environ.get('DB_USER')
    db_config['pwd'] = os.environ.get('DB_PWD')
    db_config['db_name'] = os.environ.get('DB_NAME')
    db_config['schema'] = os.environ.get('DB_SCHEMA')

    return db_config


def load_dp_config(args: 'ArgumentParser', config_path: str ='pipeline_config.yaml'):
    """Reads data pipeline configuration settings from pipeline_config.yaml file and args parser."""
    if args.config:
        config_path = args.config

    with open(config_path, 'r') as f:
        dp_config = yaml.safe_load(f)

    dp_config['db_config'] = load_db_config()
    dp_config['output_path'] = args.output
    dp_config['process_in_chunks'] = args.chunks
    if not dp_config['pipeline']['chunk_size']:
        dp_config['pipeline']['chunk_size'] = 500000

    return dp_config


def prepare_query_list(config: Dict):
    """Gathers extract_tables and custom_extracts into unified query_list. Prepares sql query for each extract_table."""
    query_list = []

    for tb in config['pipeline']['extract_tables']:
        query_list.append(
            (f'SELECT * FROM {config["db_config"]["schema"]}.{tb}', tb)
        )

    for ce in config['pipeline']['custom_extracts']:
        if ce['query'] and ce['table']:
            query_list.append(
                (ce['query'], ce['table'])
            )

    return query_list

