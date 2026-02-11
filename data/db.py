import sqlite3
from pathlib import Path
import pandas as pd
import logging


class MissingColumns(Exception):
    pass


def store_data(data, table_name, db_path):
    with sqlite3.connect(db_path) as conn:
        try:
            data.to_sql(
                name=table_name,
                con=conn,
                if_exists='replace',
                index=False
            )
        except sqlite3.OperationalError as e:
            logging.error(f'Error: {e}')
            raise



def load_table(table_name, db_path):
    with sqlite3.connect(db_path) as conn:
        try:
            table = pd.read_sql(
                f"SELECT * FROM {table_name}",
                conn
            )
            return table
        except sqlite3.OperationalError as e:
            logging.error(f'Error :{e}')
            raise



def parse_config_data(config):
    if config.db_path is None:
        config.db_path = config.default_db_path
    if config.raw_table is None:
        config.raw_table = 'unprocessed'
    if config.clean_table is None:
        config.clean_table = 'processed'
    if config.columns is None:
        logging.error(
            'Missing column names in config.yaml'
            )
        raise MissingColumns()
    return config


def create_db(config):
    config = parse_config_data(config)
    logging.info(f'Creating database: {config.db_path}')
    table_name = config.raw_table
    col_names = config.columns[:]
    col_names.append('status')
    empty_df = pd.DataFrame(columns=col_names)
    empty_df.columns = empty_df.columns.str.lower().str.strip().str.replace(' ', '_')
    try:
        with sqlite3.connect(config.db_path) as db:
            logging.info(f'Creating table: "{table_name}"')
            empty_df.to_sql(
                name=table_name,
                con=db,
                if_exists='append',
                index=False
                )
    except sqlite3.OperationalError as e:
        logging.exception(f'Error occured {e}')
        raise
    except ValueError as e:
        logging.exception(f'Erorr occured {e}')
        raise
    return config
