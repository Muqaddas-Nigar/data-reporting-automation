import logging
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import sqlite3
import shutil
from datetime import datetime
import sys
from data.db import db_manager
import hashlib


class ColumnMismatchError(Exception):
    pass


def move_processed_files(files, config):
    processed_folder = config.clean_files_loc
    time_stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    for file in files:
        new_name = f"{file.stem}_{time_stamp}{file.suffix}"
        dest = processed_folder/new_name
        try:
            shutil.move(str(file), str(dest))
            logging.info(f'Moving {file.name} to {dest.parent}')
        except PermissionError as e:
            logging.error(f'{file} cannot be moved: {e}')


def update_ingestion_log(files, file_hash_dict, config):
    logging.info('Updating ingestion log.')
    ingestion_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_record = pd.DataFrame([
        {
            'file_name': file.name,
            'file_fingerprint': file_hash_dict[file.name],
            'ingestion_time': ingestion_time
        }
        for file in files
    ]
    )
    config.table_name = config.ingestion_table_name
    old_record = db_manager('load_data', config)
    updated_record = pd.concat([old_record, new_record], ignore_index=True)
    db_manager('store_data', config, if_exists='replace', data=updated_record)


def store_in_raw_table(data, config, file):
    data.columns = data.columns.str.lower().str.strip().str.replace(' ', "_")
    cols = ["status", 'source', 'timestamp']
    for col in cols:
        if col == 'status':
            data[col] = 'Ingested'
        elif col == 'source':
            data[col] = file.name
        else:
            data[col] = datetime.now()
    config.table_name = config.raw_table
    db_manager('store_data', config, if_exists='append', data=data)


def validate_data(data, config, file):
    logging.info(f'validating data from {file.name}')
    data.columns = data.columns.str.lower().str.strip().str.replace(' ', "_")
    cols = config.data_columns
    for col in cols:
        if not col.lower().strip().replace(' ', '_') in data.columns.tolist():
            raise ColumnMismatchError
    config.table_name = 'validation_table'
    db_manager('store_data', config, if_exists='replace', data=data)



def process_raw_files(files, config):
    failed = []
    passed = []
    logging.info('Processing files.')
    for file in tqdm(files, desc="Processing raw files"):
        try:
            data = pd.read_csv(file)
            validate_data(data, config, file)
        except sqlite3.OperationalError:
            logging.warning(f'Database error while processing: {file.name} ')
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
            store_in_raw_table(data, config, file)
            passed.append(file)
    logging.info(f'Total processed: {len(files)}')
    skipped = len(failed)
    if skipped:
        logging.warning(f'Failed to process: {skipped}')
        logging.warning(
            'Unable to store files:\n' + '\n'.join(str(f.name) for f in failed)
            )
    return passed


def check_in_ingestion_log(file_hash_dict, files, config):
    config.table_name = config.ingestion_table_name
    ingestion_log = db_manager('load_data', config)
    data_files = []
    if ingestion_log.empty:
        return files
    for file in files:
        if file_hash_dict[file.name] in ingestion_log.file_fingerprint.tolist():
            logging.warning(f'Duplicate file detected:{file.name}')
            continue
        data_files.append(file)
    return data_files


def compute_files_hash(files):
    logging.info('Computing files hash.')
    file_hash = {}
    for file in files:
        with open(file, 'rb') as f:
            h = hashlib.sha256()
            while chunk := f.read(8192):
                h.update(chunk)
            file_hash[file.name] = h.hexdigest()
    return file_hash


def read_unprocessed_files(raw_files_folder):
    files = list(raw_files_folder.glob('*.csv'))
    if not files:
        logging.info(f'Raw data folder "{raw_files_folder.name}" is empty.')
        sys.exit('No files found for ingestion.')
    logging.info(f'{len(files)} file(s) found in {raw_files_folder}')
    return files


def ingest_data(config):
    files = read_unprocessed_files(config.raw_files_loc)
    file_hash_dict = compute_files_hash(files)
    files = check_in_ingestion_log(file_hash_dict, files, config)
    files = process_raw_files(files, config)
    if files:
        update_ingestion_log(files, file_hash_dict, config)
        move_processed_files(files, config)
