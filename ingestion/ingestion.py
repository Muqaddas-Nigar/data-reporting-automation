import logging
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import sqlite3
import shutil
from datetime import datetime
import sys


class MissingRawDataFolder(Exception):
    pass


class ColumnMismatchError(Exception):
    pass


def move_processed_files(files, failed_to_process, config):
    processed_folder = Path(config.clean_files_loc)
    if not Path(processed_folder).exists():
        user_provided_path = processed_folder
        processed_folder = Path(__file__).parent.parent.resolve()/'data/processed'
        logging.warning(
            f"{user_provided_path} doesn't exists"
            )
        logging.info(
            f'Moving processed files to: {processed_folder}'
            )
        processed_folder.mkdir(parents=True, exist_ok=True)
    files_to_move = [file for file in files if file not in failed_to_process]
    print(processed_folder)
    for file in files_to_move:
        new_name = f"{file.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{file.suffix}"
        dest = processed_folder/new_name
        try:
            shutil.move(str(file), str(dest))
            logging.info(f'Moving {file.name} to {dest.parent}')
        except PermissionError as e:
            logging.error(f'{file} cannot be moved: {e}')


def store_in_db(data, db_path, raw_table, file):
    data.columns = data.columns.str.lower().str.strip().str.replace(' ', "_")
    col = "status"
    data[col] = 'Pending'
    try:
        with sqlite3.connect(db_path) as db:
            logging.info(f'Storing data from "{file.name}" to "{raw_table}" ')
            data.to_sql(
                name=raw_table,
                con=db,
                if_exists='append',
                index=False
            )
    except sqlite3.OperationalError as e:
        logging.warning(f'Unable to store {file} to database: {e}')
        raise


def validate_data(data, db_path, table_name, cols):
    data.columns = data.columns.str.lower().str.strip().str.replace(' ', "_")
    for col in cols:
        if not col.lower().strip().replace(' ', '_') in data.columns.tolist():
            raise ColumnMismatchError
    try:
        with sqlite3.connect(db_path) as db:
            data.to_sql(
                name=table_name,
                con=db,
                if_exists='replace',
                index=False
            )
    except sqlite3.OperationalError:
        raise


def process_raw_files(files, config):
    failed = []
    logging.info('Processing files.')
    for file in tqdm(files, desc="Processing raw files"):
        table_name = 'validate'
        try:
            data = pd.read_csv(file)
            validate_data(data, config.db_path, table_name, config.columns)
        except sqlite3.OperationalError:
            failed.append(file)
            continue
        except ColumnMismatchError:
            failed.append(file)
            continue
        except UnicodeDecodeError:
            logging.warning(f'Unable to decode: {file.name}')
            failed.append(file)
            continue
        except pd.errors.EmptyDataError:
            logging.warning(f'File is Empty: {file.name}')
            failed.append(file)
        else:
            store_in_db(data, config.db_path, config.raw_table, file)
    logging.info(f'Total processed: {len(files)}')
    skipped = 0
    for file in failed:
        skipped += 1
    if skipped:
        logging.warning(f'Failed to process: {skipped}')
        logging.warning(
            'Unable to store files:\n' + '\n'.join(str(f.name) for f in failed)
            )
    return failed


def read_unprocessed_files(raw_files_folder):
    files = list(raw_files_folder.glob('*.csv'))
    if not files:
        logging.info(f'Raw data folder "{raw_files_folder.name}" is empty.')
        sys.exit('No files found for ingestion.')
    logging.info(f'{len(files)} file(s) found in {raw_files_folder}')
    return files


def get_data_folder(config):
    data_folder = Path(config.raw_files_loc)
    if not data_folder.resolve().exists():
        logging.error(f'Raw data folder missing: {data_folder}')
        raise MissingRawDataFolder()
    return data_folder


def ingest_data(config):
    raw_files_folder = get_data_folder(config)
    files = read_unprocessed_files(raw_files_folder)
    failed_to_process = process_raw_files(files, config)
    move_processed_files(files, failed_to_process, config)
