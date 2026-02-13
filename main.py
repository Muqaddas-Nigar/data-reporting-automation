import logging
from pathlib import Path
from config.config import load_config
from data.db import db_manager
from ingestion.ingestion import ingest_data
from cleaning.cleaner import clean_data
from config.parse_config import Config


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
    set_logger()
    config = load_config()
    config = Config(config)
    config.table_name = config.raw_table
    config.column_container = config.raw_table_columns
    config = db_manager('create_table', config, if_exists='append')
    config.table_name = 'ingestion_log'
    config.column_container = ['file_name', 'ingestion time']
    config = db_manager('create_table', config, if_exists='append')
    ingest_data(config)
    #data = load_table(config['database']['raw_table'], config['database']['path'])
    #data = clean_data(data)
    #store_data(data, config['database']['clean_table'], config['database']['path'])
    #data = load_table(config['database']['clean_table'], config['database']['path'])
    #print(data.head())




if __name__ == "__main__":
    main()
