import logging
from pathlib import Path
from config.config import load_config
from data.db import db_manager
from ingestion.ingestion import ingest_data
from config.parse_config import Config
from cleaning.cleaner import cleaner
import sys
from validator.validator import validator

def set_logger():
    log_folder = Path(__file__).parent.resolve()/'logs'
    log_file = log_folder/'log.log'
    log_folder.mkdir(parents=True, exist_ok=True)
    log_file.touch(exist_ok=True)
    logging.basicConfig(
        filename=log_file,
        filemode='a',
        level=logging.DEBUG,
        style='{',
        format="{asctime} -- {levelname} -- {message}"
    )
    logging.info('Logging configured')


def main():
    try:
        set_logger()
        config = load_config()
        config = Config(config)
        config.table_name = config.raw_table
        config.column_container = config.raw_table_columns[:]
        config = db_manager('create_table', config, if_exists='append')
        config.table_name = config.ingestion_table_name
        config.column_container = config.ingestion_table_columns
        config = db_manager('create_table', config, if_exists='append')
        ingest_data(config)
        query = f'SELECT * FROM {config.raw_table} WHERE status="Ingested"'
        config.table_name =config.raw_table
        raw_data = db_manager('load_data', config, query=query)
        if raw_data.empty:
            logging.info('No data for cleaning.')
            sys.exit('No data for cleaning.')
        data = raw_data.drop(columns=['status', 'source', 'timestamp'])
        clean_data = cleaner(data, (0.1, 0.9))
        updated_raw = validator(raw_data, clean_data)
        config.table_name = config.raw_table
        db_manager('store_data', config, if_exists='replace', data=updated_raw)
        config.table_name =config.clean_table
        db_manager('store_data', config, if_exists='append', data=clean_data)
    except Exception:
        logging.critical('Pipeline failed.')
        sys.exit('Error occurred Check log file.')


if __name__ == "__main__":
    main()
