import sqlite3
from pathlib import Path
import pandas as pd
import logging


class MissingColumns(Exception):
    pass


def store_data(db_path, table_name, data):
    with sqlite3.connect(db_path) as conn:
        try:
            data.to_sql(
                name=table_name,
                con=conn,
                if_exists='append',
                index=False
            )
        except sqlite3.OperationalError as e:
            logging.error(f'Error: {e}')
            raise


def load_table(db_path, table_name):
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


def create_db(db_path, table, columns):
    logging.info(f'Creating database: {db_path}')
    table_name = table
    col_names = columns[:]
    empty_df = pd.DataFrame(columns=col_names)
    empty_df.columns = empty_df.columns.str.lower().str.strip().str.replace(' ', '_')
    try:
        with sqlite3.connect(db_path) as db:
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
    return None


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


def db_manager(command, config, data=None, query=None):
    try:
        config = parse_config_data(config)
        db_path = config.db_path
        columns = config.columns[:]
        table_name = config.table_name
        if command == "create_db":
            print(db_path, table_name, columns, config.table_name)
            create_db(db_path, table_name, columns)
            return config
        if command == "create_table":
            #create_table(db_path, table, columns, data=None)
            pass
        if command == "load_data":
            table = load_table(db_path, table_name)
            return table
        if command == "store_data":
            store_data(db_path, table_name, data)
    except Exception as e:
        logging.exception(f'Error occured: {e}')
        raise
