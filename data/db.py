import sqlite3
import pandas as pd
import logging


def store_data(db_path, table_name, data, if_exists):
    with sqlite3.connect(db_path) as conn:
        try:
            data.to_sql(
                name=table_name,
                con=conn,
                if_exists=if_exists,
                index=False
            )
            logging.info(f'{table_name} updated successfully.')
        except sqlite3.OperationalError as e:
            logging.exception(f'Error occurred: {e}')
            raise


def load_table(db_path, table_name, query=None):
    with sqlite3.connect(db_path) as conn:
        try:
            if not query:
                statement = f'SELECT * FROM {table_name}'
            else:
                statement = query
            table = pd.read_sql(
                statement,
                conn
            )
            return table
        except sqlite3.OperationalError as e:
            logging.exception(f'Error occurred :{e}')
            raise


def create_table(db_path, table, columns, if_exists='append', data=None):
    logging.info(f'Creating database: {db_path}')
    if data is None:
        data = pd.DataFrame(columns=columns)
    data.columns = data.columns.str.lower().str.strip().str.replace(' ', '_')
    try:
        with sqlite3.connect(db_path) as db:
            logging.info(f'Creating table: "{table}"')
            data.to_sql(
                name=table,
                con=db,
                if_exists=if_exists,
                index=False
                )
    except sqlite3.OperationalError as e:
        logging.exception(f'Error occurred {e}')
        raise
    except ValueError as e:
        logging.exception(f'Error occurred {e}')
        raise
    return None


def db_manager(command, config, if_exists='append', data=None, query=None):
        db_path = config.db_path
        columns = config.column_container[:]
        table_name = config.table_name
        if command == "create_table":
            create_table(db_path, table_name, columns, if_exists, data)
            return config
        elif command == "load_data":
            table = load_table(db_path, table_name)
            return table
        elif command == "store_data":
            store_data(db_path, table_name, data, if_exists)
        else:
            logging.error('Unrecognized command for db_manager')
            raise ValueError

